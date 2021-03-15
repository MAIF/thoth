package fr.maif.projections;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.kafka.CommitterSettings;
import akka.kafka.ConsumerMessage;
import akka.kafka.ConsumerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Committer;
import akka.kafka.javadsl.Consumer;
import akka.stream.Materializer;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.RestartSource;
import akka.stream.javadsl.Sink;
import akka.stream.scaladsl.Source;
import fr.maif.eventsourcing.Event;
import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.eventsourcing.format.JacksonEventFormat;
import fr.maif.eventsourcing.format.JacksonSimpleFormat;
import fr.maif.kafka.JsonDeserializer;
import io.vavr.Tuple0;
import io.vavr.concurrent.Future;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static io.vavr.Tuple.empty;

@Slf4j
public abstract class EventuallyConsistentProjection<E extends Event, Meta, Context> {

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

    protected final ActorSystem actorSystem;
    protected final Materializer materializer;

    protected final String topic;
    protected final String groupId;
    protected final String bootstrapServers;
    protected final Duration minBackoff;
    protected final Duration maxBackoff;
    protected final double randomFactor;
    protected final Integer commitSize;
    protected final ConsumerSettings<String, EventEnvelope<E, Meta, Context>> consumerSettings;

    protected final AtomicReference<Consumer.Control> controlRef = new AtomicReference<>();
    protected final AtomicReference<Status> innerStatus = new AtomicReference<>(Status.stopped);

    public EventuallyConsistentProjection(ActorSystem actorSystem, Config<E, Meta, Context> config) {
        this.actorSystem = actorSystem;
        this.materializer = Materializer.createMaterializer(actorSystem);
        this.topic = config.topic;
        this.groupId = config.groupId;
        this.bootstrapServers = config.bootstrapServers;
        this.minBackoff = Objects.isNull(config.minBackoff) ? Duration.ofSeconds(30) : config.minBackoff;
        this.maxBackoff = Objects.isNull(config.maxBackoff) ? Duration.ofMinutes(30) : config.maxBackoff;
        this.randomFactor = Objects.isNull(config.randomFactor) ? 0.2d : config.randomFactor;
        this.commitSize = Objects.isNull(config.commitSize) ? 10 : config.commitSize;
        Function<ConsumerSettings<String, EventEnvelope<E, Meta, Context>>, ConsumerSettings<String, EventEnvelope<E, Meta, Context>>> completeConfig = Objects.isNull(config.completeConfig) ? e -> e : config.completeConfig;
        this.consumerSettings = completeConfig.apply(ConsumerSettings
                .create(actorSystem, new StringDeserializer(), deserializer())
                .withGroupId(groupId)
                .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
                .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
                .withBootstrapServers(this.bootstrapServers)
        );
        this.start();
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
        return new EventuallyConsistentProjection<E, Meta, Context>(actorSystem, config) {
            @Override
            protected String name() {
                return name;
            }

            @Override
            protected JacksonEventFormat<?, E> eventFormat() {
                return eventFormat;
            }

            @Override
            protected JacksonSimpleFormat<Meta> metaFormat() {
                return metaFormat;
            }

            @Override
            protected JacksonSimpleFormat<Context> contextFormat() {
                return contextFormat;
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

    protected abstract JacksonEventFormat<?, E> eventFormat();

    protected abstract JacksonSimpleFormat<Meta> metaFormat();

    protected abstract JacksonSimpleFormat<Context> contextFormat();

    protected Deserializer<EventEnvelope<E, Meta, Context>> deserializer() {
        return JsonDeserializer.of(eventFormat(), metaFormat(), contextFormat());
    }

    public abstract Flow<ConsumerMessage.CommittableMessage<String, EventEnvelope<E, Meta, Context>>, ConsumerMessage.CommittableOffset, NotUsed> messageHandling();


    protected Logger logger() {
        return log;
    }

    protected Status updateStatus(Status status) {
        innerStatus.set(status);
        return status;
    }

    public Status status() {
        return innerStatus.get();
    }

    public Status start() {
        Status currentStatus = status();
        if (Status.starting.equals(currentStatus) || Status.started.equals(currentStatus)) {
            logger().info("{} already started", name());
            return currentStatus;
        }
        updateStatus(Status.starting);
        CommitterSettings committerSettings = CommitterSettings.create(actorSystem);

        logger().info("Starting {} @{} on topic '{}' with group id '{}'", name(), Integer.toHexString(this.hashCode()), topic, groupId);

        RestartSource.onFailuresWithBackoff(
                minBackoff,
                maxBackoff,
                randomFactor,
                () -> {
                    logger().info("Stream for {} is starting", name());
                    return Consumer
                            .committablePartitionedSource(consumerSettings, Subscriptions.topics(topic))
                            .flatMapMerge(100, tuple ->
                                    tuple.second()
                                            .via(messageHandling())
                                            .via(Committer.flow(committerSettings.withMaxBatch(100)))
                            )
                            .mapMaterializedValue(control -> {
                                updateStatus(Status.started);
                                logger().info("Stream for {} has started", name());
                                controlRef.set(control);
                                return control;
                            })
                            .watchTermination((___, done) -> handleTerminaison(done));
                })
                .watchTermination((___, done) -> handleTerminaison(done))
                .runWith(Sink.ignore(), this.materializer);
        return Status.starting;
    }

    private CompletableFuture<Status> handleTerminaison(CompletionStage<Done> done) {
        return Future.fromCompletableFuture(done.toCompletableFuture())
                .map(any -> {
                    logger().info("Stopping {}", name());
                    return updateStatus(Status.stopped);
                })
                .recoverWith(e -> {
                    logger().error("Error during " + name(), e);
                    return Future.successful(updateStatus(Status.failed));
                })
                .recover(e -> {
                    logger().error("Error persisting " + name() + " status in db", e);
                    return Status.failed;
                })
                .flatMap(status -> stopConsumingKafka().map(any -> status))
                .toCompletableFuture();
    }


    public Future<Tuple0> stopConsumingKafka() {
        Consumer.Control control = controlRef.getAndSet(null);
        if (control != null) {
            return Future.fromCompletableFuture(control.shutdown().toCompletableFuture())
                    .onFailure(e -> logger().error("Error shutting down kafka consumer for {}", name()))
                    .onSuccess(___ -> logger().info("Kafka consumer for {} is shutdown", name()))
                    .flatMap(___ -> Future.fromCompletableFuture(control.isShutdown().toCompletableFuture()))
                    .map(__ -> empty());
        } else {
            return Future.successful(empty());
        }
    }
}
