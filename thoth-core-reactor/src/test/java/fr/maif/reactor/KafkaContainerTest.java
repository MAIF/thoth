package fr.maif.reactor;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static io.vavr.API.println;

@Testcontainers
public interface KafkaContainerTest {

    AtomicInteger counter = new AtomicInteger(0);

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


    default ReceiverOptions<String, String> receiverDefault() {
        return ReceiverOptions.<String, String>create(Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers()
                ))
                .withKeyDeserializer(new StringDeserializer())
                .withValueDeserializer(new StringDeserializer());
    }

    default SenderOptions<String, String> senderDefault() {
        return SenderOptions.<String, String>create(Map.of(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers()
                ))
                .withKeySerializer(new StringSerializer())
                .withValueSerializer(new StringSerializer());
    }

    default void produceString(String topic, String event) {
        KafkaSender.create(senderDefault()).send(Mono.just(
                SenderRecord.create(new ProducerRecord<>(
                        topic, event
                ), null)
        )).collectList().block();
    }

}
