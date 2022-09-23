package fr.maif.eventsourcing;

import fr.maif.eventsourcing.format.JacksonEventFormat;
import fr.maif.eventsourcing.format.JacksonSimpleFormat;
import fr.maif.eventsourcing.impl.DefaultAggregateStore;
import fr.maif.eventsourcing.impl.PostgresEventStore;
import fr.maif.eventsourcing.impl.ReactorKafkaEventPublisher;
import fr.maif.eventsourcing.impl.TableNames;
import io.vavr.collection.List;
import io.vavr.control.Option;
import reactor.kafka.sender.SenderOptions;

import javax.sql.DataSource;
import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.util.concurrent.Executor;

import static fr.maif.eventsourcing.EventStore.ConcurrentReplayStrategy.SKIP;

public class PostgresKafkaEventProcessor<Error, S extends State<S>, C extends Command<Meta, Context>, E extends Event, Message, Meta, Context> extends EventProcessorImpl<Error, S, C, E, Connection, Message, Meta, Context> implements Closeable {

    private final PostgresKafkaEventProcessorConfig<Error, S, C, E, Message, Meta, Context> config;

    public static PostgresKafkaEventProcessorBuilder.BuilderWithPool withDataSource(DataSource dataSource) {
        return new PostgresKafkaEventProcessorBuilder.BuilderWithPool(dataSource);
    }

    public PostgresKafkaEventProcessor(PostgresKafkaEventProcessorConfig<Error, S, C, E, Message, Meta, Context> config) {
        super(
                config.eventStore,
                config.transactionManager,
                config.aggregateStore,
                config.commandHandler,
                config.eventHandler,
                config.projections
        );
        this.config = config;
        config.eventPublisher.start(config.eventStore, Option.of(config.concurrentReplayStrategy).getOrElse(SKIP));
    }

    @Override
    public void close() throws IOException {
        this.config.eventPublisher.close();
    }

    public static class PostgresKafkaEventProcessorConfig<Error, S extends State<S>, C extends Command<Meta, Context>, E extends Event, Message, Meta, Context> {

        public final EventStore.ConcurrentReplayStrategy concurrentReplayStrategy;
        public final PostgresEventStore<E, Meta, Context> eventStore;
        public final TransactionManager<Connection> transactionManager;
        public final AggregateStore<S, String, Connection> aggregateStore;
        public final CommandHandler<Error, S, C, E, Message, Connection> commandHandler;
        public final EventHandler<S, E> eventHandler;
        public final List<Projection<Connection, E, Meta, Context>> projections;
        public final ReactorKafkaEventPublisher<E, Meta, Context> eventPublisher;

        public PostgresKafkaEventProcessorConfig(
                EventStore.ConcurrentReplayStrategy concurrentReplayStrategy,
                TableNames tableNames,
                DataSource dataSource,
                String topic,
                Executor executionContext,
                SenderOptions<String, EventEnvelope<E, Meta, Context>> producerSettings,
                TransactionManager<Connection> transactionManager,
                AggregateStore<S, String, Connection> aggregateStore, CommandHandler<Error, S, C, E, Message, Connection> commandHandler,
                EventHandler<S, E> eventHandler,
                List<Projection<Connection, E, Meta, Context>> projections,
                JacksonEventFormat<?, E> eventFormat, JacksonSimpleFormat<Meta> metaFormat,
                JacksonSimpleFormat<Context> contextFormat,
                Integer eventsBufferSize) {
            this.concurrentReplayStrategy = concurrentReplayStrategy;
            this.transactionManager = transactionManager;
            this.eventPublisher = new ReactorKafkaEventPublisher<>(producerSettings, topic, eventsBufferSize);
            this.eventStore = new PostgresEventStore<>(
                    eventPublisher,
                    dataSource,
                    executionContext,
                    tableNames,
                    eventFormat,
                    metaFormat,
                    contextFormat
            );
            this.aggregateStore = aggregateStore == null ? new DefaultAggregateStore<>(this.eventStore, eventHandler, transactionManager) : aggregateStore;
            this.commandHandler = commandHandler;
            this.eventHandler = eventHandler;
            this.projections = projections;

        }

        public PostgresKafkaEventProcessorConfig(
                TableNames tableNames,
                DataSource dataSource,
                String topic,
                SenderOptions<String, EventEnvelope<E, Meta, Context>> producerSettings,
                Executor executionContext,
                TransactionManager<Connection> transactionManager,
                AggregateStore<S, String, Connection> aggregateStore,
                CommandHandler<Error, S, C, E, Message, Connection> commandHandler,
                EventHandler<S, E> eventHandler,
                List<Projection<Connection, E, Meta, Context>> projections,
                JacksonEventFormat<?, E> eventFormat,
                JacksonSimpleFormat<Meta> metaFormat,
                JacksonSimpleFormat<Context> contextFormat, EventStore.ConcurrentReplayStrategy concurrentReplayStrategy) {
            this(tableNames, dataSource, topic, producerSettings, executionContext, transactionManager, commandHandler, eventHandler, projections, eventFormat, metaFormat, contextFormat, null, concurrentReplayStrategy);
        }

        public PostgresKafkaEventProcessorConfig(
                TableNames tableNames,
                DataSource dataSource,
                String topic,
                SenderOptions<String, EventEnvelope<E, Meta, Context>> producerSettings,
                Executor executionContext,
                TransactionManager<Connection> transactionManager,
                CommandHandler<Error, S, C, E, Message, Connection> commandHandler, EventHandler<S, E> eventHandler,
                List<Projection<Connection, E, Meta, Context>> projections,
                JacksonEventFormat<?, E> eventFormat,
                JacksonSimpleFormat<Meta> metaFormat,
                JacksonSimpleFormat<Context> contextFormat,
                Integer eventsBufferSize, EventStore.ConcurrentReplayStrategy concurrentReplayStrategy) {
            this(concurrentReplayStrategy, tableNames, dataSource, topic, executionContext, producerSettings, transactionManager, null, commandHandler, eventHandler, projections, eventFormat, metaFormat, contextFormat, eventsBufferSize);
        }

        public PostgresKafkaEventProcessorConfig(
                EventStore.ConcurrentReplayStrategy concurrentReplayStrategy, PostgresEventStore<E, Meta, Context> eventStore,
                TransactionManager<Connection> transactionManager,
                AggregateStore<S, String, Connection> aggregateStore,
                CommandHandler<Error, S, C, E, Message, Connection> commandHandler,
                EventHandler<S, E> eventHandler,
                List<Projection<Connection, E, Meta, Context>> projections,
                ReactorKafkaEventPublisher<E, Meta, Context> eventPublisher) {
            this.concurrentReplayStrategy = concurrentReplayStrategy;
            this.eventStore = eventStore;
            this.transactionManager = transactionManager;
            this.aggregateStore = aggregateStore;
            this.commandHandler = commandHandler;
            this.eventHandler = eventHandler;
            this.projections = projections;
            this.eventPublisher = eventPublisher;
        }

        public PostgresKafkaEventProcessorConfig(
                EventStore.ConcurrentReplayStrategy concurrentReplayStrategy,
                PostgresEventStore<E, Meta, Context> eventStore,
                TransactionManager<Connection> transactionManager,
                CommandHandler<Error, S, C, E, Message, Connection> commandHandler,
                EventHandler<S, E> eventHandler,
                List<Projection<Connection, E, Meta, Context>> projections,
                ReactorKafkaEventPublisher<E, Meta, Context> eventPublisher) {
            this.concurrentReplayStrategy = concurrentReplayStrategy;
            this.eventStore = eventStore;
            this.transactionManager = transactionManager;
            this.commandHandler = commandHandler;
            this.eventHandler = eventHandler;
            this.projections = projections;
            this.eventPublisher = eventPublisher;
            this.aggregateStore = new DefaultAggregateStore<>(this.eventStore, eventHandler, transactionManager);
        }
    }
}
