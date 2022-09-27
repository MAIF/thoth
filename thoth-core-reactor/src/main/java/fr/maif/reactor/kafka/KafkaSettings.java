package fr.maif.reactor.kafka;

import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.control.Option;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.SenderOptions;

import java.util.HashMap;
import java.util.Map;

public class KafkaSettings {

    private final String servers;
    private final Map<String, Object> consumerProperties;
    private final Map<String, Object> producerProperties;

    public KafkaSettings(String servers, Map<String, Object> consumerProperties, Map<String, Object> producerProperties) {
        this.servers = servers;
        this.consumerProperties = consumerProperties;
        this.producerProperties = producerProperties;
    }

    public KafkaSettings(String servers, boolean enableIdempotence, Option<String> keyStorePath, Option<String> keyStorePass, Option<String> trustStorePath, Option<String> trustStorePass) {
        this.servers = servers;
        this.producerProperties = new HashMap<>();
        this.producerProperties.put(ProducerConfig.ACKS_CONFIG, "all");
        if (enableIdempotence) {
            this.producerProperties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        }
        this.producerProperties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");

        this.consumerProperties = new HashMap<>();
        this.consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        this.consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        final Option<Tuple2<String, String>> keyStoreInfo = keyStorePath.flatMap(path -> keyStorePass.map(pass -> Tuple.of(path, pass)));
        final Option<Tuple2<String, String>> trustStoreInfo = trustStorePath.flatMap(path -> trustStorePass.map(pass -> Tuple.of(path, pass)));

        if (keyStoreInfo.isDefined() || trustStoreInfo.isDefined()) {
            consumerProperties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
            consumerProperties.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, null);
            producerProperties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
            producerProperties.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, null);
        }

        keyStoreInfo.forEach(info -> {
            String path = info._1;
            String password = info._2;
            consumerProperties.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, path);
            consumerProperties.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, password);
            consumerProperties.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, password);

            producerProperties.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, path);
            producerProperties.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, password);
            producerProperties.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, password);
        });

        trustStoreInfo.forEach(trustInfo -> {
            String trustPath = trustInfo._1;
            String trustPassword = trustInfo._2;
            consumerProperties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, trustPath);
            consumerProperties.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, trustPassword);

            producerProperties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, trustPath);
            producerProperties.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, trustPassword);
        });
    }

    public KafkaSettings(String servers) {
        this(servers, false, Option.none(), Option.none(), Option.none(), Option.none());
    }

    public ReceiverOptions<String, String> consumerSettings(String groupId) {
        return consumerSettings(groupId, new StringDeserializer());
    }

    public <S> ReceiverOptions<String, S> consumerSettings(String groupId, Deserializer<S> deserializer) {
        this.consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        this.consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.servers);
        return ReceiverOptions.<String, S>create(this.consumerProperties)
                .withKeyDeserializer(new StringDeserializer())
                .withValueDeserializer(deserializer);
    }

    public SenderOptions<String, String> producerSettings() {
        return producerSettings(new StringSerializer());
    }

    public <S> SenderOptions<String, S> producerSettings(Serializer<S> serializer) {
        this.producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.servers);
        return SenderOptions.<String, S>create(this.producerProperties)
                .withKeySerializer(new StringSerializer())
                .withValueSerializer(serializer);
    }

    public static KafkaSettingsBuilder newBuilder(String servers) {
        return new KafkaSettingsBuilder(servers);
    }

    public static class KafkaSettingsBuilder {
        private boolean enableIdempotence;
        private String keyStorePath;
        private String trustStorePath;
        private String keyStorePassword;
        private String trustStorePassword;
        private final String servers;

        private KafkaSettingsBuilder(String servers) {
            this.servers = servers;
        }

        public KafkaSettingsBuilder withKeyStoreSettings(String path, String password) {
            this.keyStorePath = path;
            this.keyStorePassword = password;
            return this;
        }

        public KafkaSettingsBuilder withTrustStoreSettings(String path, String password) {
            this.trustStorePath = path;
            this.trustStorePassword = password;

            return this;
        }

        public KafkaSettingsBuilder withEnableIdempotence(Boolean enableIdempotence) {
            this.enableIdempotence = enableIdempotence != null ? enableIdempotence : false;
            return this;
        }

        public KafkaSettings build() {
            return new KafkaSettings(servers, enableIdempotence, Option.of(keyStorePath), Option.of(keyStorePassword), Option.of(trustStorePath), Option.of(trustStorePassword));
        }
    }
}
