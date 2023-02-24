package fr.maif.reactor;

import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.eventsourcing.format.JacksonSimpleFormat;
import fr.maif.kafka.JsonSerializer;
import fr.maif.reactor.eventsourcing.KafkaEventPublisherTest;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@Testcontainers
public abstract class KafkaHelper {

    private final AtomicInteger count = new AtomicInteger(0);

    private AtomicReference<AdminClient> adminClient = new AtomicReference<>();

    @Container
    private static final KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"));

    protected String bootstrapServers() {
        return kafkaContainer.getBootstrapServers();
    }

    @BeforeEach
    protected void setUpAdminClient() {
        if(adminClient.get() == null) {
            var config = new Properties();
            config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
            this.adminClient.set(AdminClient.create(config));
        }
    }
    protected AdminClient adminClient() {
        return this.adminClient.get();
    }

    @AfterEach
    protected void cleanUpAdminClient() {
        if (adminClient() != null) {
            adminClient().close(Duration.ofSeconds(60));
            adminClient.set(null);
        }
    }

    protected ReceiverOptions<String, String> receiverOptions() {
        return ReceiverOptions.<String, String>create(Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers(),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"
                )).withValueDeserializer(new StringDeserializer())
                .withKeyDeserializer(new StringDeserializer());
    }

    private Integer nextNumber() {
        return count.incrementAndGet();
    }

    protected String createTopic() {
        return createTopic(0, 1, 1, Map.of());
    }

    protected String createTopic(Integer suffix, Integer partitions, Integer replication) {
        return createTopic(suffix, partitions, replication, Map.of());
    }

    protected String createTopic(Integer suffix, Integer partitions, Integer replication, Map<String, String> config) {
        var topicName = "topic-%s-%s".formatted(suffix, nextNumber());
        var createResult = adminClient().createTopics(
                Collections.singletonList(new NewTopic(topicName, partitions, replication.shortValue()).configs(config))
        );
        try {
            createResult.all().get(10, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
        return topicName;
    }

    protected void produceString(String topic, String event) {
        KafkaSender.create(producerSettings()).send(Mono.just(SenderRecord.create(
                new ProducerRecord<>(topic, event),
                null
        ))).blockLast();
    }

    private SenderOptions<String, String> producerSettings() {
        return producerSettings(new StringSerializer(), new StringSerializer());
    }

    private <K, V> SenderOptions<K, V> producerSettings(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        return SenderOptions.<K, V>create(
                        Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers())
                )
                .withKeySerializer(keySerializer)
                .withValueSerializer(valueSerializer);
    }
}
