package fr.maif.akka.projections;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.kafka.ConsumerMessage;
import akka.kafka.ConsumerSettings;
import akka.kafka.Subscriptions;
import akka.stream.javadsl.Flow;
import akka.stream.scaladsl.Source;
import fr.maif.eventsourcing.Event;
import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.eventsourcing.format.JacksonEventFormat;
import fr.maif.eventsourcing.format.JacksonSimpleFormat;
import fr.maif.kafka.JsonDeserializer;
import fr.maif.kafka.consumer.ResilientKafkaConsumer;
import io.vavr.Tuple0;
import io.vavr.concurrent.Future;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.function.Function;

public abstract class EventuallyConsistentProjection<E extends Event, Meta, Context> extends ResilientKafkaConsumer<String, EventEnvelope<E, Meta, Context>> {

    public static class Config<E extends Event, Meta, Context> {
        public final String topic;
        public final String groupId;
        public final String bootstrapServers;
        public final Function<ConsumerSettings<String, EventEnvelope<E, Meta, Context>>, ConsumerSettings<String, EventEnvelope<E, Meta, Context>>> completeConfig;
        public final Duration minBackoff;
        public final Duration maxBackoff;
        public final Double randomFactor;
        public final Integer commitSize;

        public static class ConfigBuilder<E extends Event, Meta, Context> {
            String topic;
            String groupId;
            String bootstrapServers;
            Function<ConsumerSettings<String, EventEnvelope<E, Meta, Context>>, ConsumerSettings<String, EventEnvelope<E, Meta, Context>>> completeConfig;
            Duration minBackoff;
            Duration maxBackoff;
            Double randomFactor;
            Integer commitSize;

            public ConfigBuilder<E, Meta, Context> topic(String topic) {
                this.topic = topic;
                return this;
            }

            public ConfigBuilder<E, Meta, Context> groupId(String groupId) {
                this.groupId = groupId;
                return this;
            }

            public ConfigBuilder<E, Meta, Context> bootstrapServers(String bootstrapServers) {
                this.bootstrapServers = bootstrapServers;
                return this;
            }

            public ConfigBuilder<E, Meta, Context> completeConfig(
                    Function<ConsumerSettings<String, EventEnvelope<E, Meta, Context>>, ConsumerSettings<String, EventEnvelope<E, Meta, Context>>> completeConfig) {
                this.completeConfig = completeConfig;
                return this;
            }

            public ConfigBuilder<E, Meta, Context> minBackoff(Duration minBackoff) {
                this.minBackoff = minBackoff;
                return this;
            }

            public ConfigBuilder<E, Meta, Context> maxBackoff(Duration maxBackoff) {
                this.maxBackoff = maxBackoff;
                return this;
            }

            public ConfigBuilder<E, Meta, Context> randomFactor(Double randomFactor) {
                this.randomFactor = randomFactor;
                return this;
            }

            public ConfigBuilder<E, Meta, Context> commitSize(Integer commitSize) {
                this.commitSize = commitSize;
                return this;
            }

            public Config<E, Meta, Context> build(){
                return new Config<>(this.topic, this.groupId, this.bootstrapServers, this.completeConfig,
                        this.minBackoff, this.maxBackoff, this.randomFactor, this.commitSize);
            }
        }

        private Config(String topic, String groupId, String bootstrapServers,
                Function<ConsumerSettings<String, EventEnvelope<E, Meta, Context>>, ConsumerSettings<String, EventEnvelope<E, Meta, Context>>> completeConfig,
                Duration minBackoff, Duration maxBackoff, Double randomFactor, Integer commitSize) {
            this.topic = topic;
            this.groupId = groupId;
            this.bootstrapServers = bootstrapServers;
            this.completeConfig = completeConfig;
            this.minBackoff = minBackoff;
            this.maxBackoff = maxBackoff;
            this.randomFactor = randomFactor;
            this.commitSize = commitSize;
        }

        public static <E extends Event, Meta, Context> Config<E, Meta, Context> create(String topic, String groupId, String bootstrapServers) {
            return Config.<E, Meta, Context>builder()
                    .bootstrapServers(bootstrapServers)
                    .groupId(groupId)
                    .topic(topic)
                    .build();
        }

        public static <E extends Event, Meta, Context> ConfigBuilder<E, Meta, Context> builder(){
            return new ConfigBuilder<>();
        }

        public ConfigBuilder<E, Meta, Context> toBuilder() {
            return Config.<E,Meta,Context>builder()
                    .topic(this.topic)
                    .groupId(this.groupId)
                    .bootstrapServers(this.bootstrapServers)
                    .completeConfig(this.completeConfig)
                    .minBackoff(this.minBackoff)
                    .maxBackoff(this.maxBackoff)
                    .randomFactor(this.randomFactor)
                    .commitSize(this.commitSize);
        }
    }

    public EventuallyConsistentProjection(ActorSystem actorSystem,
                                          JacksonEventFormat<?, E> eventFormat,
                                          JacksonSimpleFormat<Meta> metaFormat,
                                          JacksonSimpleFormat<Context> contextFormat,
                                          Config<E, Meta, Context> config) {
        super(
                actorSystem,
                ResilientKafkaConsumer.Config.<String, EventEnvelope<E, Meta, Context>>builder()
                        .minBackoff(config.minBackoff)
                        .maxBackoff(config.maxBackoff)
                        .randomFactor(defaultIfNull(config.randomFactor, 0.0d))
                        .commitSize(config.commitSize)
                        .consumerSettings(defaultIfNull(config.completeConfig, e -> e).apply(ConsumerSettings
                                .create(actorSystem, new StringDeserializer(), JsonDeserializer.of(eventFormat, metaFormat, contextFormat))
                                .withGroupId(config.groupId)
                                .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
                                .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
                                .withBootstrapServers(config.bootstrapServers)
                        ))
                        .subscription(Subscriptions.topics(config.topic))
                        .groupId(config.groupId)
                        .build()
        );

    }


    public static <E extends Event, Meta, Context> EventuallyConsistentProjection<E, Meta, Context> create(ActorSystem actorSystem,
                                                                                                           String name,
                                                                                                           Config<E, Meta, Context> config,
                                                                                                           JacksonEventFormat<?, E> eventFormat,
                                                                                                           Flow<ConsumerMessage.CommittableMessage<String, EventEnvelope<E, Meta, Context>>, ConsumerMessage.CommittableOffset, NotUsed> messageHandling) {
        return create(actorSystem, name, config, eventFormat, JacksonSimpleFormat.empty(), JacksonSimpleFormat.empty(), messageHandling);
    }

    public static <E extends Event, Meta, Context> EventuallyConsistentProjection<E, Meta, Context> create(ActorSystem actorSystem,
                                                                                                           String name,
                                                                                                           Config<E, Meta, Context> config,
                                                                                                           JacksonEventFormat<?, E> eventFormat,
                                                                                                           JacksonSimpleFormat<Meta> metaFormat,
                                                                                                           JacksonSimpleFormat<Context> contextFormat,
                                                                                                           Flow<ConsumerMessage.CommittableMessage<String, EventEnvelope<E, Meta, Context>>, ConsumerMessage.CommittableOffset, NotUsed> messageHandling) {
        return new EventuallyConsistentProjection<E, Meta, Context>(actorSystem, eventFormat, metaFormat, contextFormat, config) {
            @Override
            protected String name() {
                return name;
            }

            @Override
            public Flow<ConsumerMessage.CommittableMessage<String, EventEnvelope<E, Meta, Context>>, ConsumerMessage.CommittableOffset, NotUsed> messageHandling() {
                return messageHandling;
            }
        };
    }


    public static <E extends Event, Meta, Context> EventuallyConsistentProjection<E, Meta, Context> create(ActorSystem actorSystem,
                                                                                                           String name,
                                                                                                           Config<E, Meta, Context> config,
                                                                                                           JacksonEventFormat<?, E> eventFormat,
                                                                                                           Function<EventEnvelope<E, Meta, Context>, Future<Tuple0>> messageHandling) {
        return create(actorSystem, name, config, eventFormat, JacksonSimpleFormat.empty(), JacksonSimpleFormat.empty(), messageHandling);
    }

    public static <E extends Event, Meta, Context> EventuallyConsistentProjection<E, Meta, Context> create(ActorSystem actorSystem,
                                                                                                           String name,
                                                                                                           Config<E, Meta, Context> config,
                                                                                                           JacksonEventFormat<?, E> eventFormat,
                                                                                                           JacksonSimpleFormat<Meta> metaFormat,
                                                                                                           JacksonSimpleFormat<Context> contextFormat,
                                                                                                           Function<EventEnvelope<E, Meta, Context>, Future<Tuple0>> messageHandling) {
        return create(
                actorSystem,
                name,
                config,
                eventFormat,
                metaFormat,
                contextFormat,
                Flow.<ConsumerMessage.CommittableMessage<String, EventEnvelope<E, Meta, Context>>>create()
                        .flatMapConcat(message ->
                                Source.completionStage(messageHandling.apply(message.record().value())
                                        .map(__ -> message.committableOffset())
                                        .toCompletableFuture()
                                )
                        )
        );

    }

    protected abstract String name();

    public abstract Flow<ConsumerMessage.CommittableMessage<String, EventEnvelope<E, Meta, Context>>, ConsumerMessage.CommittableOffset, NotUsed> messageHandling();


}
