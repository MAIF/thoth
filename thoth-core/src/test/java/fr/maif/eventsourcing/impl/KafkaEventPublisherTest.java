package fr.maif.eventsourcing.impl;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.kafka.ConsumerSettings;
import akka.kafka.ProducerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.kafka.testkit.KafkaTestkitTestcontainersSettings;
import akka.kafka.testkit.javadsl.BaseKafkaTest;
import akka.kafka.testkit.javadsl.KafkaTest;
import akka.kafka.testkit.javadsl.TestcontainersKafkaTest;
import akka.stream.Materializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.javadsl.TestKit;
import com.fasterxml.jackson.databind.JsonNode;
import fr.maif.Json;
import io.vavr.API;
import io.vavr.Tuple;
import io.vavr.Tuple0;
import fr.maif.eventsourcing.Event;
import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.eventsourcing.EventStore;
import fr.maif.eventsourcing.Type;
import fr.maif.eventsourcing.format.JacksonEventFormat;
import fr.maif.eventsourcing.format.JacksonSimpleFormat;
import fr.maif.json.EventEnvelopeJson;
import fr.maif.kafka.JsonDeserializer;
import fr.maif.kafka.JsonSerializer;
import io.vavr.collection.List;
import io.vavr.concurrent.Future;
import io.vavr.control.Either;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mockito;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static fr.maif.eventsourcing.EventStore.ConcurrentReplayStrategy.NO_STRATEGY;
import static fr.maif.eventsourcing.EventStore.ConcurrentReplayStrategy.SKIP;
import static io.vavr.API.Try;
import static io.vavr.API.println;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class KafkaEventPublisherTest extends BaseKafkaTest {

    private static final ActorSystem sys = ActorSystem.create("KafkaEventPublisherTest");
    private static final Materializer mat = Materializer.createMaterializer(sys);

    KafkaEventPublisherTest() {
        super(sys, mat, "localhost:29097");
    }

    @BeforeEach
    @SneakyThrows
    void cleanUpInit() {
        setUpAdminClient();
        Set<String> topics = adminClient().listTopics().names().get(5, TimeUnit.SECONDS);
        if (!topics.isEmpty()) {
            println("Deleting "+ String.join(",", topics));
            adminClient().deleteTopics(topics).all().get();
        }
    }

    @AfterEach
    void cleanUpAfter() throws ExecutionException, InterruptedException {
        Set<String> topics = adminClient().listTopics().names().get();
        println("Deleting "+ String.join(",", topics));
        adminClient().deleteTopics(topics).all().get();
        cleanUpAdminClient();
    }

    @AfterAll
    static void afterClass() {
        TestKit.shutdownActorSystem(sys);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void eventConsumption() throws IOException, InterruptedException {

        String topic = createTopic(1, 5, 1);

        KafkaEventPublisher<TestEvent, Void, Void> publisher = createPublisher(topic);
        EventStore<Tuple0, TestEvent, Void, Void> eventStore = mock(EventStore.class);

        when(eventStore.openTransaction()).thenReturn(Future.successful(Tuple.empty()));
        when(eventStore.commitOrRollback(any(), any())).thenReturn(Future.successful(Tuple.empty()));
        when(eventStore.loadEventsUnpublished(any(), any())).thenReturn(emptyTxStream());
        when(eventStore.markAsPublished(Mockito.<List<EventEnvelope<TestEvent, Void, Void>>>any())).then(i -> Future.successful(i.getArgument(0)));

        EventEnvelope<TestEvent, Void, Void> envelope1 = eventEnvelope("value 1");
        EventEnvelope<TestEvent, Void, Void> envelope2 = eventEnvelope("value 2");
        EventEnvelope<TestEvent, Void, Void> envelope3 = eventEnvelope("value 3");

        publisher.start(eventStore, NO_STRATEGY);

        Thread.sleep(200);

        CompletionStage<List<EventEnvelope<TestEvent, Void, Void>>> results = Consumer.plainSource(consumerDefaults().withGroupId("test1"), Subscriptions.topics(topic))
                .map(ConsumerRecord::value)
                .map(KafkaEventPublisherTest::deserialize)
                .take(3)
                .map(e -> {
                    println(e);
                    return e;
                })
                .idleTimeout(Duration.of(30, ChronoUnit.SECONDS))
                .runWith(Sink.seq(), mat)
                .thenApply(List::ofAll);

        publisher.publish(List.of(
                envelope1,
                envelope2,
                envelope3
        )).get();

        List<EventEnvelope<TestEvent, Void, Void>> events = results.toCompletableFuture().join();

        assertThat(events).hasSize(3);
        assertThat(events).containsExactly(envelope1, envelope2, envelope3);

        verify(eventStore, atMost(2)).markAsPublished(Mockito.<List<EventEnvelope<TestEvent, Void, Void>>>any());

        publisher.close();
    }

    private <T> Source<T, NotUsed> emptyTxStream() {
        return Source.<T>empty();
    }

    private <T> Source<T, NotUsed> txStream(T... values) {
        return Source.<T>from(List.of(values));
    }


    @Test
    @SuppressWarnings("unchecked")
    public void eventConsumptionWithEventFromDb() throws IOException {
        String topic = createTopic(2, 5, 1);
        KafkaEventPublisher<TestEvent, Void, Void> publisher = createPublisher(topic);
        EventStore<Tuple0, TestEvent, Void, Void> eventStore = mock(EventStore.class);
        when(eventStore.openTransaction()).thenReturn(Future.successful(Tuple.empty()));
        when(eventStore.commitOrRollback(any(), any())).thenReturn(Future.successful(Tuple.empty()));
        when(eventStore.loadEventsUnpublished(any(), any())).thenReturn(txStream(
                eventEnvelope("value 1"),
                eventEnvelope("value 2"),
                eventEnvelope("value 3")
        ));
        when(eventStore.markAsPublished(eq(Tuple.empty()), Mockito.<List<EventEnvelope<TestEvent, Void, Void>>>any()))
                .then(i -> Future.successful(i.getArgument(1)));
        when(eventStore.markAsPublished(Mockito.<List<EventEnvelope<TestEvent, Void, Void>>>any()))
                .then(i -> Future.successful(i.getArgument(0)));

        publisher.start(eventStore, NO_STRATEGY);

        CompletionStage<List<String>> results = Consumer.plainSource(consumerDefaults().withGroupId("test2"), Subscriptions.topics(topic))
                .map(ConsumerRecord::value)
                .take(6)
                .idleTimeout(Duration.of(5, ChronoUnit.SECONDS))
                .runWith(Sink.seq(), mat)
                .thenApply(List::ofAll);

        publisher.publish(List.of(
                eventEnvelope("value 4"),
                eventEnvelope("value 5"),
                eventEnvelope("value 6")
        )).get();

        List<String> events = results.toCompletableFuture().join();

        assertThat(events).hasSize(6);

        verify(eventStore, times(1)).markAsPublished(any(), Mockito.<List<EventEnvelope<TestEvent, Void, Void>>>any());
        verify(eventStore, atLeastOnce()).markAsPublished(Mockito.<List<EventEnvelope<TestEvent, Void, Void>>>any());

        publisher.close();
    }



    @Test
    @SuppressWarnings("unchecked")
    public void testRestart() throws IOException {
        AtomicBoolean failed = new AtomicBoolean(false);
        String topic = createTopic(3, 5, 1);
        KafkaEventPublisher<TestEvent, Void, Void> publisher = createPublisher(topic);
        EventStore<Tuple0, TestEvent, Void, Void> eventStore = mock(EventStore.class);
        when(eventStore.openTransaction()).thenReturn(Future.successful(Tuple.empty()));
        when(eventStore.commitOrRollback(any(), any())).thenReturn(Future.successful(Tuple.empty()));

        EventEnvelope<TestEvent, Void, Void> envelope1 = eventEnvelope("value 1");
        EventEnvelope<TestEvent, Void, Void> envelope2 = eventEnvelope("value 2");
        EventEnvelope<TestEvent, Void, Void> envelope3 = eventEnvelope("value 3");

        when(eventStore.loadEventsUnpublished(any(), any())).thenReturn(txStream(envelope1, envelope2, envelope3));
        when(eventStore.markAsPublished(Mockito.<List<EventEnvelope<TestEvent, Void, Void>>>any())).thenAnswer(in -> Future.successful(in.getArgument(0)));
        when(eventStore.markAsPublished(any(), Mockito.<List<EventEnvelope<TestEvent, Void, Void>>>any()))
                .then(i -> {
                    if (failed.getAndSet(true)) {
                        return Future.successful(i.getArgument(1));
                    } else {
                        throw new RuntimeException("Oups");
                    }
                });

        publisher.start(eventStore, SKIP);

        CompletionStage<List<EventEnvelope<TestEvent, Void, Void>>> results = Consumer.plainSource(consumerDefaults().withGroupId("test3"), Subscriptions.topics(topic))
                .map(ConsumerRecord::value)
                .map(KafkaEventPublisherTest::deserialize)
                .take(6)
                .idleTimeout(Duration.of(10, ChronoUnit.SECONDS))
                .runWith(Sink.seq(), mat)
                .thenApply(List::ofAll);

        List<EventEnvelope<TestEvent, Void, Void>> events = results.toCompletableFuture().join();

        assertThat(events).hasSize(6);

        println(events.mkString("\n"));

        assertThat(events).containsExactly(envelope1, envelope2, envelope3, envelope1, envelope2, envelope3);

        verify(eventStore, times(2)).markAsPublished(any(), Mockito.<List<EventEnvelope<TestEvent, Void, Void>>>any());

        publisher.close();
    }


    private static EventEnvelope<TestEvent, Void, Void> deserialize(String event) {
        return EventEnvelopeJson.deserialize(event, new TestEventSerializer(), JacksonSimpleFormat.empty(), JacksonSimpleFormat.empty(), (s, o) -> {
            println("Error " + s + " - " + o);
        }, e -> {});
    }

    static AtomicLong sequence = new AtomicLong();

    private EventEnvelope<TestEvent, Void, Void> eventEnvelope(String value) {
        long sequenceNum = sequence.incrementAndGet();
        String entityId = "entityId";
        return EventEnvelope.<TestEvent, Void, Void>builder()
                .withEmissionDate(LocalDateTime.now())
                .withId(UUID.randomUUID())
                .withEntityId(entityId)
                .withSequenceNum(sequenceNum)
                .withEvent(new TestEvent(value, entityId))
                .build();
    }

    private KafkaEventPublisher<TestEvent, Void, Void> createPublisher(String topic) {
        return new KafkaEventPublisher<>(sys, producerSettings(), topic, null, Duration.of(500, ChronoUnit.MILLIS), Duration.of(30, ChronoUnit.SECONDS));
    }

    private ProducerSettings<String, EventEnvelope<TestEvent, Void, Void>> producerSettings() {
        return ProducerSettings.create(
                sys,
                new StringSerializer(),
                new JsonSerializer<TestEvent, Void, Void>(
                        new TestEventSerializer(),
                        JacksonSimpleFormat.empty(),
                        JacksonSimpleFormat.empty()
                )
        ).withBootstrapServers(bootstrapServers());
    }

    private ConsumerSettings<String, EventEnvelope<TestEvent, Void, Void>> consumerSettings() {
        return ConsumerSettings.create(
                sys,
                new StringDeserializer(),
                new JsonDeserializer<TestEvent, Void, Void>(
                        new TestEventSerializer(),
                        JacksonSimpleFormat.empty(),
                        JacksonSimpleFormat.empty(),
                        (s, o) -> {},
                        e -> {}
                )
        ).withBootstrapServers(bootstrapServers());
    }

    public static class TestEventSerializer implements JacksonEventFormat<String, TestEvent> {
        @Override
        public Either<String, TestEvent> read(String type, Long version, JsonNode json) {
            return Either.right(new TestEvent(json.get("value").asText(), "entityId"));
        }

        @Override
        public JsonNode write(TestEvent json) {
            return Json.newObject().put("value", json.value);
        }
    }

    public static class TestEvent implements Event {

        public final String value;
        public final String id;

        public TestEvent(String value, String id) {
            this.value = value;
            this.id = id;
        }

        @Override
        public Type<?> type() {
            return Type.create(TestEvent.class, 1L);
        }

        @Override
        public String entityId() {
            return id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TestEvent testEvent = (TestEvent) o;
            return Objects.equals(value, testEvent.value) && Objects.equals(id, testEvent.id);
        }

        @Override
        public int hashCode() {
            return Objects.hash(value, id);
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", TestEvent.class.getSimpleName() + "[", "]")
                    .add("value='" + value + "'")
                    .add("id='" + id + "'")
                    .toString();
        }
    }

}
