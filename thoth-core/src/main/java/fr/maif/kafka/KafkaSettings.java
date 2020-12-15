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
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaSettings {
    private final String servers;

    private final Option<String> keyStorePath;
    private final Option<String> trustStorePath;
    private final Option<String> keyStorePass;
    private final Option<String> trustStorePass;

    public KafkaSettings(String servers, Option<String> keyStorePath, Option<String> keyStorePass, Option<String> trustStorePath, Option<String> trustStorePass) {
        this.servers = servers;
        this.keyStorePath = keyStorePath;
        this.trustStorePath = trustStorePath;
        this.trustStorePass = trustStorePass;
        this.keyStorePass = keyStorePass;
    }

    public static KafkaSettings fromConfig(Config config) {
        return new KafkaSettings(
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

        final ConsumerSettings<String, S> settings = ConsumerSettings
                .create(system, new StringDeserializer(), deserializer)
                .withGroupId(groupId)
                .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
                .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
                .withBootstrapServers(this.servers);

        final Option<Tuple2<String, String>> keyStoreInfo = this.keyStorePath.flatMap(path -> this.keyStorePass.map(pass -> Tuple.of(path, pass)));
        final Option<Tuple2<String, String>> trustStoreInfo = this.trustStorePath.flatMap(path -> this.trustStorePass.map(pass -> Tuple.of(path, pass)));

        return keyStoreInfo.map(info -> {
            String path = info._1;
            String password = info._2;
            final ConsumerSettings<String, S> settingsWithKeyStore = settings.withProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL")
                    .withProperty(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, "required")
                    .withProperty(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, null)
                    .withProperty(SslConfigs.SSL_KEY_PASSWORD_CONFIG, password)
                    .withProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, path)
                    .withProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, password);

            return trustStoreInfo.map(trustInfo -> {
                String trustPath = trustInfo._1;
                String trustPassword = trustInfo._2;

                return settingsWithKeyStore.withProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, trustPath)
                        .withProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, trustPassword);
            }).getOrElse(settingsWithKeyStore);
        }).getOrElse(settings);
    }

    public ProducerSettings<String, String> producerSettings(ActorSystem system) {
        return producerSettings(system, new StringSerializer());
    }

    public <S> ProducerSettings<String, S> producerSettings(ActorSystem system, Serializer<S> serializer) {
        ProducerSettings<String, S> settings = ProducerSettings
                .create(system, new StringSerializer(), serializer)
                .withBootstrapServers(this.servers);

        final Option<Tuple2<String, String>> keyStoreInfo = this.keyStorePath.flatMap(path -> this.keyStorePass.map(pass -> Tuple.of(path, pass)));
        final Option<Tuple2<String, String>> trustStoreInfo = this.trustStorePath.flatMap(path -> this.trustStorePass.map(pass -> Tuple.of(path, pass)));

        ProducerSettings<String, S> keyStoreSettings = keyStoreInfo.map(info -> {
            String path = info._1;
            String password = info._2;
            return settings.withProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL")
                    .withProperty(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, "required")
                    .withProperty(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, null)
                    .withProperty(SslConfigs.SSL_KEY_PASSWORD_CONFIG, password)
                    .withProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, path)
                    .withProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, password);
        }).getOrElse(settings);

        return trustStoreInfo.map(trustInfo -> {
            String trustPath = trustInfo._1;
            String trustPassword = trustInfo._2;

            return keyStoreSettings.withProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, trustPath)
                    .withProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, trustPassword);
        }).getOrElse(keyStoreSettings);
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
