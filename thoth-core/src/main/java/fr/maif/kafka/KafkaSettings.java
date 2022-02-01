package fr.maif.kafka;

import akka.actor.ActorSystem;
import akka.kafka.ConsumerSettings;
import akka.kafka.ProducerSettings;
import com.typesafe.config.Config;
import fr.maif.config.Configs;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.control.Option;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaSettings {

    private final String servers;
    private final Map<String, String> consumerProperties;
    private final Map<String, String> producerProperties;

    public KafkaSettings(String servers, Map<String, String> consumerProperties, Map<String, String> producerProperties) {
        this.servers = servers;
        this.consumerProperties = consumerProperties;
        this.producerProperties = producerProperties;
    }

    public KafkaSettings(String servers, Option<String> keyStorePath, Option<String> keyStorePass, Option<String> trustStorePath, Option<String> trustStorePass) {
        this.servers = servers;
        this.producerProperties = new HashMap<>();
        this.producerProperties.put(ProducerConfig.ACKS_CONFIG, "all");
        this.producerProperties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
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

    public static fr.maif.kafka.KafkaSettings fromConfig(Config config) {
        return new fr.maif.kafka.KafkaSettings(
                config.getString("kafka.servers"),
                Configs.getOptionalString(config, "kafka.keystore.location"),
                Configs.getOptionalString(config, "kafka.truststore.location"),
                Configs.getOptionalString(config, "kafka.truststore.pass"),
                Configs.getOptionalString(config, "kafka.keystore.pass")
        );
    }

    public KafkaSettings(String servers) {
        this(servers, Option.none(), Option.none(), Option.none(), Option.none());
    }

    public ConsumerSettings<String, String> consumerSettings(ActorSystem system, String groupId) {
        return consumerSettings(system, groupId, new StringDeserializer());
    }

    public <S> ConsumerSettings<String, S> consumerSettings(ActorSystem system, String groupId, Deserializer<S> deserializer) {
        return ConsumerSettings
                .create(system, new StringDeserializer(), deserializer)
                .withGroupId(groupId)
                .withProperties(this.consumerProperties)
                .withBootstrapServers(this.servers);
    }

    public ProducerSettings<String, String> producerSettings(ActorSystem system) {
        return producerSettings(system, new StringSerializer());
    }

    public <S> ProducerSettings<String, S> producerSettings(ActorSystem system, Serializer<S> serializer) {
        return ProducerSettings
                .create(system, new StringSerializer(), serializer)
                .withProperties(this.producerProperties)
                .withBootstrapServers(this.servers);
    }

    public static KafkaSettingsBuilder newBuilder(String servers) {
        return new KafkaSettingsBuilder(servers);
    }

    public static class KafkaSettingsBuilder {
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

        public KafkaSettings build() {
            return new KafkaSettings(servers, Option.of(keyStorePath), Option.of(keyStorePassword), Option.of(trustStorePath), Option.of(trustStorePassword));
        }
    }
}
