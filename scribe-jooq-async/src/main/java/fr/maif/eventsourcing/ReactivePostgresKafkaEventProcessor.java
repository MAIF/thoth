package fr.maif.eventsourcing;

import akka.actor.ActorSystem;
import akka.kafka.ProducerSettings;
import fr.maif.eventsourcing.format.JacksonEventFormat;
import fr.maif.eventsourcing.format.JacksonSimpleFormat;
import fr.maif.eventsourcing.impl.DefaultAggregateStore;
import fr.maif.eventsourcing.impl.KafkaEventPublisher;
import fr.maif.jooq.PgAsyncPool;
import fr.maif.jooq.PgAsyncTransaction;
import io.vavr.collection.List;

import java.io.Closeable;
import java.io.IOException;

public class ReactivePostgresKafkaEventProcessor<Error, S extends State<S>, C extends Command<Meta, Context>, E extends Event, Message, Meta, Context> extends EventProcessor<Error, S, C, E, PgAsyncTransaction, Message, Meta, Context> implements Closeable {

    private final PostgresKafkaEventProcessorConfig<Error, S, C, E, Message, Meta, Context> config;

    public ReactivePostgresKafkaEventProcessor(PostgresKafkaEventProcessorConfig<Error, S, C, E, Message, Meta, Context> config) {
        super(
                config.eventStore,
                config.transactionManager,
                config.aggregateStore,
                config.commandHandler,
                config.eventHandler,
                config.projections
        );
        this.config = config;
        config.eventPublisher.start(config.eventStore);
    }

    @Override
    public void close() throws IOException {
        this.config.eventPublisher.close();
    }

    public static class PostgresKafkaEventProcessorConfig<Error, S extends State<S>, C extends Command<Meta, Context>, E extends Event, Message, Meta, Context> {

        public final ReactivePostgresEventStore<E, Meta, Context> eventStore;
        public final TransactionManager<PgAsyncTransaction> transactionManager;
        public final AggregateStore<S, String, PgAsyncTransaction> aggregateStore;
        public final CommandHandler<Error, S, C, E, Message, PgAsyncTransaction> commandHandler;
        public final EventHandler<S, E> eventHandler;
        public final List<Projection<PgAsyncTransaction, E, Meta, Context>> projections;
        public final KafkaEventPublisher<E, Meta, Context> eventPublisher;

        public PostgresKafkaEventProcessorConfig(
                ActorSystem system,
                TableNames tableNames,
                PgAsyncPool pgAsyncPool,
                String topic,
                ProducerSettings<String, EventEnvelope<E, Meta, Context>> producerSettings,
                TransactionManager<PgAsyncTransaction> transactionManager,
                AggregateStore<S, String, PgAsyncTransaction> aggregateStore,
                CommandHandler<Error, S, C, E, Message, PgAsyncTransaction> commandHandler,
                EventHandler<S, E> eventHandler,
                List<Projection<PgAsyncTransaction, E, Meta, Context>> projections,
                JacksonEventFormat<?, E> eventFormat, JacksonSimpleFormat<Meta> metaFormat,
                JacksonSimpleFormat<Context> contextFormat,
                Integer eventsBufferSize) {
            this.transactionManager = transactionManager;
            this.eventPublisher = new KafkaEventPublisher<>(system, producerSettings, topic, eventsBufferSize);
            this.eventStore = new ReactivePostgresEventStore<>(
                    system,
                    eventPublisher,
                    pgAsyncPool,
                    tableNames,
                    eventFormat,
                    metaFormat,
                    contextFormat
            );
            this.aggregateStore = aggregateStore == null ? new DefaultAggregateStore<>(this.eventStore, eventHandler, system, transactionManager) : aggregateStore;
            this.commandHandler = commandHandler;
            this.eventHandler = eventHandler;
            this.projections = projections;

        }

        public PostgresKafkaEventProcessorConfig(
                ActorSystem system,
                TableNames tableNames,
                PgAsyncPool pgAsyncPool,
                String topic,
                ProducerSettings<String, EventEnvelope<E, Meta, Context>> producerSettings,
                TransactionManager<PgAsyncTransaction> transactionManager,
                AggregateStore<S, String, PgAsyncTransaction> aggregateStore,
                CommandHandler<Error, S, C, E, Message, PgAsyncTransaction> commandHandler,
                EventHandler<S, E> eventHandler,
                List<Projection<PgAsyncTransaction, E, Meta, Context>> projections,
                JacksonEventFormat<?, E> eventFormat,
                JacksonSimpleFormat<Meta> metaFormat,
                JacksonSimpleFormat<Context> contextFormat) {
            this(system, tableNames, pgAsyncPool, topic, producerSettings, transactionManager, aggregateStore, commandHandler, eventHandler, projections, eventFormat, metaFormat, contextFormat, null);
        }

        public PostgresKafkaEventProcessorConfig(
                ActorSystem system,
                TableNames tableNames,
                PgAsyncPool pgAsyncPool,
                String topic,
                ProducerSettings<String, EventEnvelope<E, Meta, Context>> producerSettings,
                TransactionManager<PgAsyncTransaction> transactionManager,
                CommandHandler<Error, S, C, E, Message, PgAsyncTransaction> commandHandler,
                EventHandler<S, E> eventHandler,
                List<Projection<PgAsyncTransaction, E, Meta, Context>> projections,
                JacksonEventFormat<?, E> eventFormat,
                JacksonSimpleFormat<Meta> metaFormat,
                JacksonSimpleFormat<Context> contextFormat,
                Integer eventsBufferSize) {
            this(system, tableNames, pgAsyncPool, topic, producerSettings, transactionManager, null, commandHandler, eventHandler, projections, eventFormat, metaFormat, contextFormat, eventsBufferSize);
        }

        public PostgresKafkaEventProcessorConfig(
                ReactivePostgresEventStore<E, Meta, Context> eventStore,
                TransactionManager<PgAsyncTransaction> transactionManager,
                AggregateStore<S, String, PgAsyncTransaction> aggregateStore,
                CommandHandler<Error, S, C, E, Message, PgAsyncTransaction> commandHandler,
                EventHandler<S, E> eventHandler,
                List<Projection<PgAsyncTransaction, E, Meta, Context>> projections,
                KafkaEventPublisher<E, Meta, Context> eventPublisher) {
            this.eventStore = eventStore;
            this.transactionManager = transactionManager;
            this.aggregateStore = aggregateStore;
            this.commandHandler = commandHandler;
            this.eventHandler = eventHandler;
            this.projections = projections;
            this.eventPublisher = eventPublisher;
        }
    }
}
