package fr.maif.eventsourcing;

import akka.actor.ActorSystem;
import akka.kafka.ProducerSettings;
import fr.maif.akka.AkkaExecutionContext;
import fr.maif.eventsourcing.format.JacksonEventFormat;
import fr.maif.eventsourcing.format.JacksonSimpleFormat;
import fr.maif.eventsourcing.impl.DefaultAggregateStore;
import fr.maif.eventsourcing.impl.KafkaEventPublisher;
import fr.maif.eventsourcing.impl.PostgresEventStore;
import fr.maif.eventsourcing.impl.TableNames;
import io.vavr.collection.List;

import javax.sql.DataSource;
import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;

public class PostgresKafkaEventProcessor<Error, S extends State<S>, C extends Command<Meta, Context>, E extends Event, Message, Meta, Context> extends EventProcessor<Error, S, C, E, Connection, Message, Meta, Context> implements Closeable {

    private final PostgresKafkaEventProcessorConfig<Error, S, C, E, Message, Meta, Context> config;

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
        config.eventPublisher.start(config.eventStore);
    }

    @Override
    public void close() throws IOException {
        this.config.eventPublisher.close();
    }

    public static class PostgresKafkaEventProcessorConfig<Error, S extends State<S>, C extends Command<Meta, Context>, E extends Event, Message, Meta, Context> {

        public final PostgresEventStore<E, Meta, Context> eventStore;
        public final TransactionManager<Connection> transactionManager;
        public final AggregateStore<S, String, Connection> aggregateStore;
        public final CommandHandler<Error, S, C, E, Message, Connection> commandHandler;
        public final EventHandler<S, E> eventHandler;
        public final List<Projection<Connection, E, Meta, Context>> projections;
        public final KafkaEventPublisher<E, Meta, Context> eventPublisher;

        public PostgresKafkaEventProcessorConfig(
                ActorSystem system,
                TableNames tableNames,
                DataSource dataSource,
                String topic,
                ProducerSettings<String, EventEnvelope<E, Meta, Context>> producerSettings,
                AkkaExecutionContext executionContext,
                TransactionManager<Connection> transactionManager,
                AggregateStore<S, String, Connection> aggregateStore, CommandHandler<Error, S, C, E, Message, Connection> commandHandler,
                EventHandler<S, E> eventHandler,
                List<Projection<Connection, E, Meta, Context>> projections,
                JacksonEventFormat<?, E> eventFormat, JacksonSimpleFormat<Meta> metaFormat,
                JacksonSimpleFormat<Context> contextFormat,
                Integer eventsBufferSize) {
            this.transactionManager = transactionManager;
            this.eventPublisher = new KafkaEventPublisher<>(system, producerSettings, topic, eventsBufferSize);
            this.eventStore = new PostgresEventStore<>(
                    system,
                    eventPublisher,
                    dataSource,
                    executionContext,
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
                DataSource dataSource,
                String topic,
                ProducerSettings<String, EventEnvelope<E, Meta, Context>> producerSettings,
                AkkaExecutionContext executionContext,
                TransactionManager<Connection> transactionManager,
                AggregateStore<S, String, Connection> aggregateStore,
                CommandHandler<Error, S, C, E, Message, Connection> commandHandler,
                EventHandler<S, E> eventHandler,
                List<Projection<Connection, E, Meta, Context>> projections,
                JacksonEventFormat<?, E> eventFormat,
                JacksonSimpleFormat<Meta> metaFormat,
                JacksonSimpleFormat<Context> contextFormat) {
            this(system, tableNames, dataSource, topic, producerSettings, executionContext, transactionManager, commandHandler, eventHandler, projections, eventFormat, metaFormat, contextFormat, null);
        }

        public PostgresKafkaEventProcessorConfig(
                ActorSystem system,
                TableNames tableNames,
                DataSource dataSource,
                String topic,
                ProducerSettings<String, EventEnvelope<E, Meta, Context>> producerSettings,
                AkkaExecutionContext executionContext,
                TransactionManager<Connection> transactionManager,
                CommandHandler<Error, S, C, E, Message, Connection> commandHandler, EventHandler<S, E> eventHandler,
                List<Projection<Connection, E, Meta, Context>> projections,
                JacksonEventFormat<?, E> eventFormat,
                JacksonSimpleFormat<Meta> metaFormat,
                JacksonSimpleFormat<Context> contextFormat,
                Integer eventsBufferSize) {
            this(system, tableNames, dataSource, topic, producerSettings, executionContext, transactionManager, null, commandHandler, eventHandler, projections, eventFormat, metaFormat, contextFormat, eventsBufferSize);
        }

        public PostgresKafkaEventProcessorConfig(
                PostgresEventStore<E, Meta, Context> eventStore,
                TransactionManager<Connection> transactionManager,
                AggregateStore<S, String, Connection> aggregateStore,
                CommandHandler<Error, S, C, E, Message, Connection> commandHandler,
                EventHandler<S, E> eventHandler,
                List<Projection<Connection, E, Meta, Context>> projections,
                KafkaEventPublisher<E, Meta, Context> eventPublisher) {
            this.eventStore = eventStore;
            this.transactionManager = transactionManager;
            this.aggregateStore = aggregateStore;
            this.commandHandler = commandHandler;
            this.eventHandler = eventHandler;
            this.projections = projections;
            this.eventPublisher = eventPublisher;
        }

        public PostgresKafkaEventProcessorConfig(
                ActorSystem actorSystem,
                PostgresEventStore<E, Meta, Context> eventStore,
                TransactionManager<Connection> transactionManager,
                CommandHandler<Error, S, C, E, Message, Connection> commandHandler,
                EventHandler<S, E> eventHandler,
                List<Projection<Connection, E, Meta, Context>> projections,
                KafkaEventPublisher<E, Meta, Context> eventPublisher) {
            this.eventStore = eventStore;
            this.transactionManager = transactionManager;
            this.commandHandler = commandHandler;
            this.eventHandler = eventHandler;
            this.projections = projections;
            this.eventPublisher = eventPublisher;
            this.aggregateStore = new DefaultAggregateStore<>(this.eventStore, eventHandler, actorSystem, transactionManager);
        }
    }
}
