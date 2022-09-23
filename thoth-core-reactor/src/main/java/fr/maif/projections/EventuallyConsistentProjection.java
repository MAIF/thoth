package fr.maif.projections;

import fr.maif.eventsourcing.Event;
import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.eventsourcing.format.JacksonEventFormat;
import fr.maif.eventsourcing.format.JacksonSimpleFormat;
import fr.maif.kafka.JsonDeserializer;
import fr.maif.kafka.reactor.consumer.ResilientKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

public abstract class EventuallyConsistentProjection<E extends Event, Meta, Context> extends ResilientKafkaConsumer<String, EventEnvelope<E, Meta, Context>> {

    public static class Config<E extends Event, Meta, Context> {
        public final String topic;
        public final String groupId;
        public final String bootstrapServers;
        public final Function<ReceiverOptions<String, EventEnvelope<E, Meta, Context>>, ReceiverOptions<String, EventEnvelope<E, Meta, Context>>> completeConfig;
        public final Duration minBackoff;
        public final Duration maxBackoff;
        public final Double randomFactor;
        public final Integer commitSize;

        public static class ConfigBuilder<E extends Event, Meta, Context> {
            String topic;
            String groupId;
            String bootstrapServers;
            Function<ReceiverOptions<String, EventEnvelope<E, Meta, Context>>, ReceiverOptions<String, EventEnvelope<E, Meta, Context>>> completeConfig;
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
                    Function<ReceiverOptions<String, EventEnvelope<E, Meta, Context>>, ReceiverOptions<String, EventEnvelope<E, Meta, Context>>> completeConfig) {
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
                Function<ReceiverOptions<String, EventEnvelope<E, Meta, Context>>, ReceiverOptions<String, EventEnvelope<E, Meta, Context>>> completeConfig,
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

    public EventuallyConsistentProjection(JacksonEventFormat<?, E> eventFormat,
                                          JacksonSimpleFormat<Meta> metaFormat,
                                          JacksonSimpleFormat<Context> contextFormat,
                                          Config<E, Meta, Context> config) {
        super(
                ResilientKafkaConsumer.Config.<String, EventEnvelope<E, Meta, Context>>builder()
                        .minBackoff(config.minBackoff)
                        .maxBackoff(config.maxBackoff)
                        .randomFactor(defaultIfNull(config.randomFactor, 0.0d))
                        .commitSize(config.commitSize)
                        .consumerSettings(defaultIfNull(config.completeConfig, e -> e).apply(ReceiverOptions
                                .<String, EventEnvelope<E, Meta, Context>>create(Map.of(
                                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers,
                                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest",
                                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false",
                                        ConsumerConfig.GROUP_ID_CONFIG, config.groupId
                                ))
                                .withKeyDeserializer(new StringDeserializer())
                                .withValueDeserializer(JsonDeserializer.of(eventFormat, metaFormat, contextFormat))
                                .subscription(List.of(config.topic))
                        ))
                        .build()
        );

    }

    protected abstract String name();

    public abstract Function<Flux<ReceiverRecord<String, EventEnvelope<E, Meta, Context>>>, Flux<ReceiverRecord<String, EventEnvelope<E, Meta, Context>>>> messageHandling();


    public static <E extends Event, Meta, Context> EventuallyConsistentProjection<E, Meta, Context> create(String name,
                                                                                                           Config<E, Meta, Context> config,
                                                                                                           JacksonEventFormat<?, E> eventFormat,
                                                                                                           MessageHandling<E, Meta, Context> messageHandling) {
        return create(name, config, eventFormat, JacksonSimpleFormat.empty(), JacksonSimpleFormat.empty(), messageHandling);
    }

    public static <E extends Event, Meta, Context> EventuallyConsistentProjection<E, Meta, Context> create(String name,
                                                                                                           Config<E, Meta, Context> config,
                                                                                                           JacksonEventFormat<?, E> eventFormat,
                                                                                                           JacksonSimpleFormat<Meta> metaFormat,
                                                                                                           JacksonSimpleFormat<Context> contextFormat,
                                                                                                           MessageHandling<E, Meta, Context> messageHandling) {
        return new EventuallyConsistentProjection<E, Meta, Context>(eventFormat, metaFormat, contextFormat, config) {
            @Override
            protected String name() {
                return name;
            }


            @Override
            public Function<Flux<ReceiverRecord<String, EventEnvelope<E, Meta, Context>>>, Flux<ReceiverRecord<String, EventEnvelope<E, Meta, Context>>>> messageHandling() {
                return messageHandling;
            }
        };
    }


    public static <E extends Event, Meta, Context> EventuallyConsistentProjection<E, Meta, Context> simpleHandler(String name,
                                                                                                           Config<E, Meta, Context> config,
                                                                                                           JacksonEventFormat<?, E> eventFormat,
                                                                                                           Function<EventEnvelope<E, Meta, Context>, CompletionStage<Void>> messageHandling) {
        return simpleHandler(name, config, eventFormat, JacksonSimpleFormat.empty(), JacksonSimpleFormat.empty(), messageHandling);
    }

    public static <E extends Event, Meta, Context> EventuallyConsistentProjection<E, Meta, Context> simpleHandler(String name,
                                                                                                           Config<E, Meta, Context> config,
                                                                                                           JacksonEventFormat<?, E> eventFormat,
                                                                                                           JacksonSimpleFormat<Meta> metaFormat,
                                                                                                           JacksonSimpleFormat<Context> contextFormat,
                                                                                                           Function<EventEnvelope<E, Meta, Context>, CompletionStage<Void>> messageHandling) {
        return create(
                name,
                config,
                eventFormat,
                metaFormat,
                contextFormat,
                (Flux<ReceiverRecord<String, EventEnvelope<E, Meta, Context>>> it) -> it.concatMap(message ->
                                Mono.fromCompletionStage(() -> messageHandling.apply(message.value())
                                        .thenApply(__ -> message)
                                )
                        )
        );

    }


    interface MessageHandling<E extends Event, Meta, Context> extends Function<Flux<ReceiverRecord<String, EventEnvelope<E, Meta, Context>>>, Flux<ReceiverRecord<String, EventEnvelope<E, Meta, Context>>>> {

    }

}
