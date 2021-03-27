package fr.maif.projections;

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
import fr.maif.kafka.consumer.KafkaConsumerWithRetries;
import io.vavr.Tuple0;
import io.vavr.concurrent.Future;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.function.Function;

@Slf4j
public abstract class EventuallyConsistentProjection<E extends Event, Meta, Context> extends KafkaConsumerWithRetries<String, EventEnvelope<E, Meta, Context>> {

    @Builder(toBuilder = true)
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Config<E extends Event, Meta, Context> {
        public final String topic;
        public final String groupId;
        public final String bootstrapServers;
        public final Function<ConsumerSettings<String, EventEnvelope<E, Meta, Context>>, ConsumerSettings<String, EventEnvelope<E, Meta, Context>>> completeConfig;
        public final Duration minBackoff;
        public final Duration maxBackoff;
        public final Integer randomFactor;
        public final Integer commitSize;

        public static <E extends Event, Meta, Context> Config<E, Meta, Context> create(String topic, String groupId, String bootstrapServers) {
            return Config.<E, Meta, Context>builder()
                    .bootstrapServers(bootstrapServers)
                    .groupId(groupId)
                    .topic(topic)
                    .build();
        }
    }

    public EventuallyConsistentProjection(ActorSystem actorSystem,
                                          JacksonEventFormat<?, E> eventFormat,
                                          JacksonSimpleFormat<Meta> metaFormat,
                                          JacksonSimpleFormat<Context> contextFormat,
                                          Config<E, Meta, Context> config) {
        super(
                actorSystem,
                KafkaConsumerWithRetries.Config.<String, EventEnvelope<E, Meta, Context>>builder()
                        .minBackoff(config.minBackoff)
                        .maxBackoff(config.maxBackoff)
                        .randomFactor(config.randomFactor)
                        .commitSize(config.commitSize)
                        .consumerSettings(defaultIfNull(config.completeConfig, e -> e).apply(ConsumerSettings
                                .create(actorSystem, new StringDeserializer(), JsonDeserializer.of(eventFormat, metaFormat, contextFormat))
                                .withGroupId(config.groupId)
                                .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
                                .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
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
