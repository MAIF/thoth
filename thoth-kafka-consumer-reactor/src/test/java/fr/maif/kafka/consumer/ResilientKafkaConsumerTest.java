package fr.maif.kafka.consumer;

import akka.actor.ActorSystem;
import akka.kafka.testkit.javadsl.TestcontainersKafkaTest;
import akka.stream.Materializer;
import akka.testkit.javadsl.TestKit;
import fr.maif.kafka.reactor.consumer.ResilientKafkaConsumer;
import fr.maif.kafka.reactor.consumer.Status;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverOptions;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ResilientKafkaConsumerTest extends TestcontainersKafkaTest {
    private static final ActorSystem system = ActorSystem.create("test");
    private ReceiverOptions<String, String> receiverOptions;

    public ResilientKafkaConsumerTest() {
        super(system, Materializer.createMaterializer(system));
    }

    @BeforeEach
    public void init() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        receiverOptions = ReceiverOptions.<String, String>create(props);
    }

    @Test
    void consumer() throws Exception {

        String topic = createTopic();
        String groupId = "test-group-id-1";

        AtomicReference<String> names = new AtomicReference<>("");

        ResilientKafkaConsumer<String, String> resilientKafkaConsumer = ResilientKafkaConsumer.create(
                "test",
                ResilientKafkaConsumer.Config.create(
                        List.of(topic),
                        groupId,
                        receiverOptions
                ),
                event -> {
                    System.out.println("Event %s".formatted(event.value()));
                    names.set(names.get() + " " + event.value());
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
    void crash() throws Exception {

        String topic = createTopic();
        String groupId = "test-group-id-3";

        AtomicReference<String> names = new AtomicReference<>("");
        AtomicBoolean hasCrashed = new AtomicBoolean(false);

        ResilientKafkaConsumer.createFromFlux(
                "test",
                ResilientKafkaConsumer.Config.create(List.of(topic), groupId, receiverOptions).withCommitSize(1).withMinBackoff(Duration.ofMillis(20)),
                it -> it
                        .index()
                        .concatMap(messageAndIndex -> {
                            System.out.println(messageAndIndex.getT1() + " - " + messageAndIndex.getT2().value());
                            if (messageAndIndex.getT1() > 0 && !hasCrashed.get()) {
                                System.out.println("Crash !");
                                hasCrashed.set(true);
                                return Flux.error(new RuntimeException("Oups"));
                            } else {
                                names.set(names.get() + " " + messageAndIndex.getT2().value());
                                return Flux.just(messageAndIndex.getT2());
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
    void consumerLifecycle() throws Exception {

        String topic = createTopic();
        String groupId = "test-group-id-4";

        AtomicReference<String> names = new AtomicReference<>("");
        AtomicBoolean isStarted = new AtomicBoolean(false);
        AtomicBoolean isStarting = new AtomicBoolean(false);
        AtomicBoolean isStopping = new AtomicBoolean(false);
        AtomicBoolean isStopped = new AtomicBoolean(false);
        AtomicBoolean isFailed = new AtomicBoolean(false);

        ResilientKafkaConsumer<String, String> resilientKafkaConsumer = ResilientKafkaConsumer.create(
                "test",
                ResilientKafkaConsumer.Config.create(
                                List.of(topic),
                                groupId,
                                receiverOptions
                        )
                        .withOnStarting(() -> Mono.fromRunnable(() -> {
                            isStarting.set(true);
                        }))
                        .withOnStarted((c, time) -> Mono.fromRunnable(() -> {
                            isStarted.set(true);
                        }))
                        .withOnStopping(() -> Mono.fromRunnable(() -> {
                            isStopping.set(true);
                        }))
                        .withOnStopped(() -> Mono.fromRunnable(() -> {
                            isStopped.set(true);
                        }))
                        .withOnFailed(e -> Mono.fromRunnable(() -> {
                            isFailed.set(true);
                        }))
                , event -> {
                    names.set(names.get() + " " + event.value());
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
        resilientKafkaConsumer.stop().block();
        assertThat(resilientKafkaConsumer.status()).isIn(Status.Stopping, Status.Stopped);
        Thread.sleep(10000);
        assertThat(resilientKafkaConsumer.status()).isIn(Status.Stopped);
        assertThat(isStopped.get()).isTrue();
    }


    @AfterAll
    void shutdownActorSystem() {
        TestKit.shutdownActorSystem(system);
    }

}