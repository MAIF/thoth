package fr.maif.kafka.consumer;

import akka.Done;
import akka.actor.ActorSystem;
import akka.kafka.ConsumerMessage.CommittableMessage;
import akka.kafka.ConsumerMessage.CommittableOffset;
import akka.kafka.ConsumerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.testkit.javadsl.TestcontainersKafkaTest;
import akka.stream.Materializer;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.FlowWithContext;
import akka.testkit.javadsl.TestKit;
import lombok.SneakyThrows;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ResilientKafkaConsumerTest extends TestcontainersKafkaTest {
    private static final ActorSystem system = ActorSystem.create("test");

    public ResilientKafkaConsumerTest() {
        super(system, Materializer.createMaterializer(system));
    }


    @Test
    @SneakyThrows
    void consumer() {

        String topic = createTopic();
        String groupId = "test-group-id-1";

        AtomicReference<String> names = new AtomicReference<>("");

        ResilientKafkaConsumer<String, String> resilientKafkaConsumer = ResilientKafkaConsumer.create(
                system,
                "test",
                ResilientKafkaConsumer.Config.create(
                        Subscriptions.topics(topic),
                        groupId,
                        ConsumerSettings
                                .create(system, new StringDeserializer(), new StringDeserializer())
                                .withBootstrapServers(bootstrapServers())
                ),
                event -> {
                    names.set(names.get() + " " + event.record().value());
                }
        );

        Status status = resilientKafkaConsumer.status();

        Thread.sleep(3000);

        resultOf(produceString(topic, "event-1"));
        resultOf(produceString(topic, "event-2"));
        resultOf(produceString(topic, "event-3"));

        Thread.sleep(10000);

        String actual = names.get();
        System.out.println(actual);
        assertThat(actual).isEqualTo(" event-1 event-2 event-3");

    }

    @Test
    @SneakyThrows
    void contexteAkkastreamApi() {

        String topic = createTopic();
        String groupId = "test-group-id-3";

        AtomicReference<String> names = new AtomicReference<>("");

        ResilientKafkaConsumer.createFromFlowCtxAgg(
                system,
                "test",
                ResilientKafkaConsumer.Config.create(
                        Subscriptions.topics(topic),
                        groupId,
                        ConsumerSettings
                                .create(system, new StringDeserializer(), new StringDeserializer())
                                .withBootstrapServers(bootstrapServers())
                ),
                FlowWithContext.<CommittableMessage<String, String>, CommittableOffset>create()
                        .grouped(3)
                        .map(messages -> {
                            String collectedMessages = messages.stream().map(m -> m.record().value()).collect(Collectors.joining(" "));
                            names.set(collectedMessages);
                            return Done.done();
                        })
        );
        Thread.sleep(3000);

        resultOf(produceString(topic, "event-1"));
        resultOf(produceString(topic, "event-2"));
        resultOf(produceString(topic, "event-3"));

        Thread.sleep(10000);

        String actual = names.get();
        System.out.println(actual);
        assertThat(actual).isEqualTo("event-1 event-2 event-3");

    }


    @Test
    @SneakyThrows
    void crash() {

        String topic = createTopic();
        String groupId = "test-group-id-3";

        AtomicReference<String> names = new AtomicReference<>("");
        AtomicBoolean hasCrashed = new AtomicBoolean(false);

        ResilientKafkaConsumer.createFromFlow(
                system,
                "test",
                ResilientKafkaConsumer.Config.create(
                        Subscriptions.topics(topic),
                        groupId,
                        ConsumerSettings
                                .create(system, new StringDeserializer(), new StringDeserializer())
                                .withBootstrapServers(bootstrapServers())
                ).withCommitSize(1).withMinBackoff(Duration.ofMillis(20)),
                Flow
                        .<CommittableMessage<String, String>>create()
                        .zipWithIndex()
                        .mapAsync(1, messageAndIndex -> {
                            System.out.println(messageAndIndex.second() + " - " + messageAndIndex.first().record().value());
                            if (messageAndIndex.second() > 0 && !hasCrashed.get()) {
                                System.out.println("Crash !");
                                hasCrashed.set(true);
                                CompletableFuture<CommittableOffset> completableFuture = new CompletableFuture<>();
                                completableFuture.completeExceptionally(new RuntimeException("Oups"));
                                return completableFuture;
                            } else {
                                names.set(names.get() + " " + messageAndIndex.first().record().value());
                                return CompletableFuture.completedFuture(messageAndIndex.first().committableOffset());
                            }
                        })
        );
        Thread.sleep(4000);

        resultOf(produceString(topic, "event-1"));
        resultOf(produceString(topic, "event-2"));
        resultOf(produceString(topic, "event-3"));

        Thread.sleep(60000);

        String actual = names.get();
        System.out.println(actual);
        assertThat(actual).isEqualTo(" event-1 event-2 event-3");
        assertThat(hasCrashed.get()).isTrue();

    }

    @Test
    @SneakyThrows
    void consumerLifecycle() {

        String topic = createTopic();
        String groupId = "test-group-id-4";

        AtomicReference<String> names = new AtomicReference<>("");
        AtomicBoolean isStarted = new AtomicBoolean(false);
        AtomicBoolean isStarting = new AtomicBoolean(false);
        AtomicBoolean isStopping = new AtomicBoolean(false);
        AtomicBoolean isStopped = new AtomicBoolean(false);
        AtomicBoolean isFailed = new AtomicBoolean(false);

        ResilientKafkaConsumer<String, String> resilientKafkaConsumer = ResilientKafkaConsumer.create(
                system,
                "test",
                ResilientKafkaConsumer.Config
                        .create(
                                Subscriptions.topics(topic),
                                groupId,
                                ConsumerSettings
                                        .create(system, new StringDeserializer(), new StringDeserializer())
                                        .withBootstrapServers(bootstrapServers())
                        )
                        .withOnStarting(() -> CompletableFuture.supplyAsync(() -> {
                            isStarting.set(true);
                            return Done.done();
                        }))
                        .withOnStarted((c, time) -> CompletableFuture.supplyAsync(() -> {
                            isStarted.set(true);
                            return Done.done();
                        }))
                        .withOnStopping(c -> CompletableFuture.supplyAsync(() -> {
                            isStopping.set(true);
                            return Done.done();
                        }))
                        .withOnStopped(() -> CompletableFuture.supplyAsync(() -> {
                            isStopped.set(true);
                            return Done.done();
                        }))
                        .withOnFailed(e -> CompletableFuture.supplyAsync(() -> {
                            isFailed.set(true);
                            return Done.done();
                        }))
                , event -> {
                    names.set(names.get() + " " + event.record().value());
                }
        );

        Status status = resilientKafkaConsumer.status();
        assertThat(status).isIn(Status.Started, Status.Starting);
        Thread.sleep(50);
        assertThat(isStarting.get()).isTrue();
        Thread.sleep(3000);
        assertThat(resilientKafkaConsumer.status()).isIn(Status.Started);
        assertThat(isStarted.get()).isTrue();

        resultOf(produceString(topic, "event-1"));
        resultOf(produceString(topic, "event-2"));
        resultOf(produceString(topic, "event-3"));

        Thread.sleep(10000);

        String actual = names.get();
        System.out.println(actual);
        assertThat(actual).isEqualTo(" event-1 event-2 event-3");
        CompletionStage<Done> stop = resilientKafkaConsumer.stop();
        assertThat(resilientKafkaConsumer.status()).isIn(Status.Stopping, Status.Stopped);
        stop.toCompletableFuture().join();
        assertThat(isStopping.get()).isTrue();
        assertThat(resilientKafkaConsumer.status()).isIn(Status.Stopped);
        assertThat(isStopped.get()).isTrue();
    }


    @AfterAll
    void shutdownActorSystem() {
        TestKit.shutdownActorSystem(system);
    }

}