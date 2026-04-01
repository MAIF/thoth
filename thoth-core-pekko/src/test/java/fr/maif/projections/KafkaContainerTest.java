package fr.maif.projections;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.kafka.ConsumerSettings;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static io.vavr.API.println;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;

@Testcontainers
public interface KafkaContainerTest {

    AtomicInteger counter = new AtomicInteger(0);

    @Container
    KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"))
            .withStartupAttempts(2);

    static void startContainer() {
        kafkaContainer.start();
    }

    default String bootstrapServers() {
        return kafkaContainer.getBootstrapServers();
    }

    default Admin adminClient() {
        return Admin.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers()
        ));
    }

    default String createTopic() {
        return createTopic("topic-"+counter.incrementAndGet(), 3, 1);
    }

    default String createTopic(String name, int partitions, int replication) {
        try {
            CreateTopicsResult createTopicsResult = adminClient().createTopics(java.util.List.of(new NewTopic(name, partitions, (short) replication)));
            createTopicsResult.all().get(10, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException("Unable to create topic with name " + name, e);
        }
        return name;
    }

    default void deleteTopics() {
        try {
            Set<String> topics = adminClient().listTopics().names().get(5, TimeUnit.SECONDS);
            if (!topics.isEmpty()) {
                println("Deleting " + String.join(",", topics));
                adminClient().deleteTopics(topics).all().get();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    default ConsumerSettings<String, String> consumerDefaults(ActorSystem actorSystem) {
        return ConsumerSettings.create(actorSystem, new StringDeserializer(), new StringDeserializer())
                .withBootstrapServers(kafkaContainer.getBootstrapServers())
                .withGroupId("test-group-id")
                .withProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    default KafkaProducer<String, String> producer() {
        return new KafkaProducer<>(Map.of(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers()
                ), new StringSerializer(), new StringSerializer());
    }

    default void produceString(String topic, String event) {
        CompletableFuture<String> completableFuture = new CompletableFuture<>();
        producer().send(new ProducerRecord<>(
                topic, event
        ), (recordMetadata, e) -> {
            if (e != null) {
                completableFuture.completeExceptionally(e);
            } else {
                completableFuture.complete(recordMetadata.topic());
            }
        });
        completableFuture.join();
    }

}
