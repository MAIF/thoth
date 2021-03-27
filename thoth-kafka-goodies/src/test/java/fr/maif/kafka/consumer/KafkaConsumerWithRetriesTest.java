package fr.maif.kafka.consumer;

import akka.Done;
import akka.actor.ActorSystem;
import akka.kafka.ConsumerMessage;
import akka.kafka.ConsumerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.testkit.javadsl.TestcontainersKafkaTest;
import akka.stream.Materializer;
import akka.stream.javadsl.Flow;
import akka.testkit.javadsl.TestKit;
import lombok.SneakyThrows;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class KafkaConsumerWithRetriesTest extends TestcontainersKafkaTest {
    private static final ActorSystem system = ActorSystem.create("test");

    public KafkaConsumerWithRetriesTest() {
        super(system, Materializer.createMaterializer(system));
    }


    @Test
    @SneakyThrows
    void consumer() {

        String topic = createTopic();
        String groupId = "test-group-id";

        AtomicReference<String> names = new AtomicReference<>("");

        KafkaConsumerWithRetries.create(
                system,
                "test",
                KafkaConsumerWithRetries.Config.create(
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
    void crash() {

        String topic = createTopic();
        String groupId = "test-group-id";

        AtomicReference<String> names = new AtomicReference<>("");
        AtomicBoolean hasCrashed = new AtomicBoolean(false);

        KafkaConsumerWithRetries<String, String> kafkaConsumer = KafkaConsumerWithRetries.create(
                system,
                "test",
                KafkaConsumerWithRetries.Config.create(
                        Subscriptions.topics(topic),
                        groupId,
                        ConsumerSettings
                                .create(system, new StringDeserializer(), new StringDeserializer())
                                .withBootstrapServers(bootstrapServers())
                ).withCommitSize(1).withMinBackoff(Duration.ofMillis(20)),
                Flow
                        .<ConsumerMessage.CommittableMessage<String, String>>create()
                        .zipWithIndex()
                        .mapAsync(1, messageAndIndex -> {
                            System.out.println(messageAndIndex.second() + " - " + messageAndIndex.first().record().value());
                            if (messageAndIndex.second() > 0 && !hasCrashed.get()) {
                                System.out.println("Crash !");
                                hasCrashed.set(true);
                                CompletableFuture<ConsumerMessage.CommittableOffset> completableFuture = new CompletableFuture<>();
                                completableFuture.completeExceptionally(new RuntimeException("Oups"));
                                return completableFuture;
                            } else {
                                names.set(names.get() + " " + messageAndIndex.first().record().value());
                                return CompletableFuture.completedFuture(messageAndIndex.first().committableOffset());
                            }
                        })
        );
        Thread.sleep(3000);

        resultOf(produceString(topic, "event-1"));
        resultOf(produceString(topic, "event-2"));
        resultOf(produceString(topic, "event-3"));

        Thread.sleep(60000);

        String actual = names.get();
        System.out.println(actual);
        assertThat(actual).isEqualTo(" event-1 event-2 event-3");
        assertThat(hasCrashed.get()).isTrue();

    }


    @AfterAll
    void shutdownActorSystem() {
        TestKit.shutdownActorSystem(system);
    }

}