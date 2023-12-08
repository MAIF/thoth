package fr.maif.reactor.eventsourcing;

import com.fasterxml.jackson.databind.JsonNode;
import fr.maif.concurrent.CompletionStages;
import fr.maif.eventsourcing.Event;
import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.eventsourcing.Type;
import fr.maif.eventsourcing.format.JacksonEventFormat;
import fr.maif.eventsourcing.format.JacksonSimpleFormat;
import fr.maif.json.EventEnvelopeJson;
import fr.maif.json.Json;
import fr.maif.kafka.JsonDeserializer;
import fr.maif.kafka.JsonSerializer;
import fr.maif.reactor.KafkaContainerTest;
import io.vavr.API;
import io.vavr.collection.List;
import io.vavr.control.Either;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.SenderOptions;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static fr.maif.eventsourcing.EventStore.ConcurrentReplayStrategy.NO_STRATEGY;
import static fr.maif.eventsourcing.EventStore.ConcurrentReplayStrategy.SKIP;
import static io.vavr.API.List;
import static io.vavr.API.println;
import static java.util.function.Function.identity;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@Testcontainers
public class KafkaEventPublisherTest implements KafkaContainerTest {


    @BeforeAll
    public static void setUp() {
        KafkaContainerTest.startContainer();
    }

    @BeforeEach
    @AfterEach
    void cleanUpInit() {
        sequence.set(0);
        deleteTopics();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void eventConsumption() throws IOException, InterruptedException {

        String topic = createTopic("eventConsumption", 5, 1);

        ReactorKafkaEventPublisher<TestEvent, Void, Void> publisher = createPublisher(topic);
        InMemoryEventStore<TestEvent, Void, Void> eventStore = spy(new InMemoryEventStore<>());

        EventEnvelope<TestEvent, Void, Void> envelope1 = eventEnvelopeUnpublished("value 1");
        EventEnvelope<TestEvent, Void, Void> envelope2 = eventEnvelopeUnpublished("value 2");
        EventEnvelope<TestEvent, Void, Void> envelope3 = eventEnvelopeUnpublished("value 3");

        publisher.start(eventStore, NO_STRATEGY);

        Thread.sleep(200);

        CompletionStage<List<EventEnvelope<TestEvent, Void, Void>>> results = KafkaReceiver.create(receiverDefault()
                        .consumerProperty(ConsumerConfig.GROUP_ID_CONFIG, "eventConsumption")
                        .consumerProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                        .subscription(List.of(topic).toJavaList()))
                .receive()
                .map(ConsumerRecord::value)
                .map(KafkaEventPublisherTest::deserialize)
                .bufferTimeout(50, Duration.ofSeconds(4))
                .map(e -> {
                    println(e);
                    return e;
                })
                .take(1)
                .timeout(Duration.of(30, ChronoUnit.SECONDS))
                .collectList()
                .map(l -> List.ofAll(l).flatMap(identity()))
                .toFuture();

        publisher.publish(List.of(
                envelope1,
                envelope2,
                envelope3
        )).toCompletableFuture().join();

        List<EventEnvelope<TestEvent, Void, Void>> events = results.toCompletableFuture().join();

        assertThat(events).hasSize(3);
        assertThat(events).containsExactly(envelope1, envelope2, envelope3);

        verify(eventStore, atMost(2)).markAsPublished(Mockito.<List<EventEnvelope<TestEvent, Void, Void>>>any());

        publisher.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void eventConsumptionWithEventFromDb() throws IOException, InterruptedException {

        String topic = createTopic("eventConsumptionWithEventFromDb", 5, 1);

        ReactorKafkaEventPublisher<TestEvent, Void, Void> publisher = createPublisher(topic);
        InMemoryEventStore<TestEvent, Void, Void> eventStore = spy(new InMemoryEventStore<>(
                eventEnvelopeUnpublished("value 1"),
                eventEnvelopeUnpublished("value 2"),
                eventEnvelopeUnpublished("value 3")
        ));

        publisher.start(eventStore, NO_STRATEGY);

        Thread.sleep(200);

        CompletionStage<List<String>> results = KafkaReceiver.create(receiverDefault()
                        .consumerProperty(ConsumerConfig.GROUP_ID_CONFIG, "eventConsumptionWithEventFromDb")
                        .consumerProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                        .subscription(List.of(topic).toJavaList())
                )
                .receive()
                .map(ConsumerRecord::value)
                .bufferTimeout(50, Duration.ofSeconds(4))
                .take(1)
                .timeout(Duration.of(30, ChronoUnit.SECONDS))
                .collectList()
                .map(l -> List.ofAll(l).flatMap(identity()))
                .toFuture();

        publisher.publish(List.of(
                eventEnvelopeUnpublished("value 4"),
                eventEnvelopeUnpublished("value 5"),
                eventEnvelopeUnpublished("value 6")
        )).toCompletableFuture().join();

        List<String> events = results.toCompletableFuture().join();

        assertThat(events).hasSize(6);

        verify(eventStore, times(1)).markAsPublished(any(), Mockito.<List<EventEnvelope<TestEvent, Void, Void>>>any());
        verify(eventStore, atLeastOnce()).markAsPublished(Mockito.<List<EventEnvelope<TestEvent, Void, Void>>>any());

        publisher.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testRestartWithMock() throws IOException, InterruptedException {

        AtomicInteger failed = new AtomicInteger(0);
        AtomicInteger streamCount = new AtomicInteger(0);
        String topic = createTopic("testRestart", 5, 1);
        ReactorKafkaEventPublisher<TestEvent, Void, Void> publisher = createPublisher(topic);

        Supplier<Flux<EventEnvelope<TestEvent, Void, Void>>> eventsFlux = () -> KafkaReceiver
                .create(receiverDefault()
                        .consumerProperty(ConsumerConfig.GROUP_ID_CONFIG, "testRestart")
                        .consumerProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                        .consumerProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
                        .subscription(List.of(topic).toJavaList()))
                .receive()
                .doOnNext(e -> e.receiverOffset().acknowledge())
                .map(ConsumerRecord::value)
                .map(KafkaEventPublisherTest::deserialize);

        EventEnvelope<TestEvent, Void, Void> envelope1 = eventEnvelopeUnpublished("value 1");
        EventEnvelope<TestEvent, Void, Void> envelope2 = eventEnvelopeUnpublished("value 2");
        EventEnvelope<TestEvent, Void, Void> envelope3 = eventEnvelopeUnpublished("value 3");
        EventEnvelope<TestEvent, Void, Void> envelope4 = eventEnvelopeUnpublished("value 4");
        EventEnvelope<TestEvent, Void, Void> envelope5 = eventEnvelopeUnpublished("value 5");
        EventEnvelope<TestEvent, Void, Void> envelope6 = eventEnvelopeUnpublished("value 6");
        EventEnvelope<TestEvent, Void, Void> envelope7 = eventEnvelopeUnpublished("value 7");
        EventEnvelope<TestEvent, Void, Void> envelope8 = eventEnvelopeUnpublished("value 8");
        EventEnvelope<TestEvent, Void, Void> envelope9 = eventEnvelopeUnpublished("value 9");
        EventEnvelope<TestEvent, Void, Void> envelope10 = eventEnvelopeUnpublished("value 10");
        EventEnvelope<TestEvent, Void, Void> envelope11 = eventEnvelopeUnpublished("value 11");
        EventEnvelope<TestEvent, Void, Void> envelope12 = eventEnvelopeUnpublished("value 12");

        InMemoryEventStore<TestEvent, Void, Void> eventStore = spy(new InMemoryEventStore<>(
                () -> {
                    if (failed.incrementAndGet() > 1) {
                        return CompletionStages.successful(API.Tuple());
                    } else {
                        return CompletionStages.failed(new RuntimeException("Oups "+failed.get()));
                    }
                }, () -> {
                    int count = streamCount.incrementAndGet();
                    if (count == 1) {
                        return CompletionStages.failed(new RuntimeException("Oups stream "+count));
                    } else {
                        return CompletionStages.successful(API.Tuple());
                    }
                },
                envelope1, envelope2, envelope3
        ));

        publisher.start(eventStore, SKIP);

        Thread.sleep(200);


        CompletionStage<List<EventEnvelope<TestEvent, Void, Void>>> results = eventsFlux.get()
                .bufferTimeout(50, Duration.ofSeconds(4))
                .take(1)
                .timeout(Duration.of(30, ChronoUnit.SECONDS))
                .collectList()
                .map(l -> List.ofAll(l).flatMap(identity()))
                .toFuture();

        List<EventEnvelope<TestEvent, Void, Void>> toPublish = List(envelope4, envelope5, envelope6);
        eventStore.publish(toPublish);
        publisher.publish(toPublish);

        List<EventEnvelope<TestEvent, Void, Void>> events = results.toCompletableFuture().join();

        println(events.mkString("\n"));

        assertThat(events).usingRecursiveFieldByFieldElementComparator().containsExactly(
                // Event that were in store when publisher started
                envelope1, envelope2, envelope3,
                // First transaction failed so, events were replayed
                envelope1, envelope2, envelope3,
                // Inqueued event were published but transaction failed
                // So events were replayed
                envelope4, envelope5, envelope6);
        assertThat(eventStore.store.values()).containsExactly(published(envelope1, envelope2, envelope3, envelope4, envelope5, envelope6));
//
        verify(eventStore, times(2)).openTransaction();
        verify(eventStore, times(2)).markAsPublished(any(), Mockito.<List<EventEnvelope<TestEvent, Void, Void>>>any());
        verify(eventStore, times(0)).markAsPublished(Mockito.<List<EventEnvelope<TestEvent, Void, Void>>>any());



        List<EventEnvelope<TestEvent, Void, Void>> toPublishFailInMemory = List(envelope7, envelope8, envelope9);
        eventStore.publish(toPublishFailInMemory);
        publisher.publish(toPublishFailInMemory);
        List<EventEnvelope<TestEvent, Void, Void>> resultsAfterCrash = eventsFlux.get()
                .bufferTimeout(50, Duration.ofSeconds(10))
                .take(1)
                .timeout(Duration.of(30, ChronoUnit.SECONDS))
                .collectList()
                .map(l -> List.ofAll(l).flatMap(identity()))
                .block();

        println(resultsAfterCrash.mkString("\n"));
        assertThat(resultsAfterCrash).contains(envelope7, envelope8, envelope9);
        verify(eventStore, times(3)).openTransaction();
        verify(eventStore, times(3)).markAsPublished(any(), Mockito.<List<EventEnvelope<TestEvent, Void, Void>>>any());
        verify(eventStore, times(1)).markAsPublished(Mockito.<List<EventEnvelope<TestEvent, Void, Void>>>any());

        List<EventEnvelope<TestEvent, Void, Void>> toPublishFailAtTheEnd = List(envelope10, envelope11, envelope12);
        publisher.publish(toPublishFailAtTheEnd);

        List<EventEnvelope<TestEvent, Void, Void>> resultsAfterCrashInMemory = eventsFlux.get()
                .bufferTimeout(50, Duration.ofSeconds(10))
                .take(1)
                .timeout(Duration.of(30, ChronoUnit.SECONDS))
                .collectList()
                .map(l -> List.ofAll(l).flatMap(identity()))
                .block();

        println(resultsAfterCrashInMemory.mkString("\n"));
        assertThat(resultsAfterCrashInMemory).containsExactly(envelope10, envelope11, envelope12);

        verify(eventStore, times(3)).openTransaction();
        verify(eventStore, times(3)).markAsPublished(any(), Mockito.<List<EventEnvelope<TestEvent, Void, Void>>>any());
        verify(eventStore, times(2)).markAsPublished(Mockito.<List<EventEnvelope<TestEvent, Void, Void>>>any());

        publisher.close();
    }

    @SafeVarargs
    private EventEnvelope<TestEvent, Void, Void>[] published(EventEnvelope<TestEvent, Void, Void>... envelopes) {
        return List.of(envelopes).map(e -> e.copy().withPublished(true).build()).toJavaArray(EventEnvelope[]::new);
    }

    private static EventEnvelope<TestEvent, Void, Void> deserialize(String event) {
        return EventEnvelopeJson.deserialize(event, new TestEventSerializer(), JacksonSimpleFormat.empty(), JacksonSimpleFormat.empty(), (s, o) -> {
            println("Error " + s + " - " + o);
        }, e -> {
        });
    }

    static AtomicLong sequence = new AtomicLong();

    private EventEnvelope<TestEvent, Void, Void> eventEnvelopeUnpublished(String value) {
        long sequenceNum = sequence.incrementAndGet();
        String entityId = "entityId";
        return EventEnvelope.<TestEvent, Void, Void>builder()
                .withEmissionDate(LocalDateTime.now())
                .withId(UUID.randomUUID())
                .withEntityId(entityId)
                .withSequenceNum(sequenceNum)
                .withPublished(false)
                .withEvent(new TestEvent(value, entityId))
                .build();
    }

    private ReactorKafkaEventPublisher<TestEvent, Void, Void> createPublisher(String topic) {
        return new ReactorKafkaEventPublisher<>(producerSettings(), topic, null, Duration.of(500, ChronoUnit.MILLIS), Duration.of(30, ChronoUnit.SECONDS));
    }

    private SenderOptions<String, EventEnvelope<TestEvent, Void, Void>> producerSettings() {
        return SenderOptions.<String, EventEnvelope<TestEvent, Void, Void>>create(
                        Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers())
                )
                .withKeySerializer(new StringSerializer())
                .withValueSerializer(new JsonSerializer<TestEvent, Void, Void>(
                        new TestEventSerializer(),
                        JacksonSimpleFormat.empty(),
                        JacksonSimpleFormat.empty()
                ));
    }

    private ReceiverOptions<String, EventEnvelope<TestEvent, Void, Void>> receiverOptions() {
        return ReceiverOptions.<String, EventEnvelope<TestEvent, Void, Void>>create(Map.of(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers()
                ))
                .withKeyDeserializer(new StringDeserializer())
                .withValueDeserializer(
                        new JsonDeserializer<TestEvent, Void, Void>(
                                new TestEventSerializer(),
                                JacksonSimpleFormat.empty(),
                                JacksonSimpleFormat.empty(),
                                (s, o) -> {
                                },
                                e -> {
                                }
                        )
                );
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
