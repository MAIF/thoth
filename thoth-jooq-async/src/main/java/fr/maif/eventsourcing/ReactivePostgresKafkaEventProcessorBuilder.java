package fr.maif.eventsourcing;

import akka.actor.ActorSystem;
import akka.kafka.ProducerSettings;
import fr.maif.eventsourcing.format.JacksonEventFormat;
import fr.maif.eventsourcing.format.JacksonSimpleFormat;
import fr.maif.eventsourcing.impl.DefaultAggregateStore;
import fr.maif.eventsourcing.impl.KafkaEventPublisher;
import fr.maif.jooq.PgAsyncPool;
import fr.maif.jooq.PgAsyncTransaction;
import io.vavr.Tuple0;
import io.vavr.collection.List;
import lombok.AllArgsConstructor;

public class ReactivePostgresKafkaEventProcessorBuilder {


    @AllArgsConstructor
    public static class BuilderWithSystem {
        protected final ActorSystem system;

        public BuilderWithPool withPgAsyncPool(PgAsyncPool pgAsyncPool) {
            return new BuilderWithPool(system, pgAsyncPool);
        }
    }

    @AllArgsConstructor
    public static class BuilderWithPool {
        protected final ActorSystem system;
        protected final PgAsyncPool pgAsyncPool;

        public BuilderWithTables withTables(TableNames tableNames) {
            return new BuilderWithTables(system, pgAsyncPool, tableNames);
        }
    }

    @AllArgsConstructor
    public static class BuilderWithTables {
        protected final ActorSystem system;
        protected final PgAsyncPool pgAsyncPool;
        protected final TableNames tableNames;

        public BuilderWithTx withExistingTransactionManager(TransactionManager<PgAsyncTransaction> transactionManager) {
            return new BuilderWithTx(system, pgAsyncPool, tableNames, transactionManager);
        }

        public BuilderWithTx withTransactionManager() {
            return new BuilderWithTx(system, pgAsyncPool, tableNames, new ReactiveTransactionManager(pgAsyncPool));
        }
    }

    @AllArgsConstructor
    public static class BuilderWithTx {
        protected final ActorSystem system;
        protected final PgAsyncPool pgAsyncPool;
        protected final TableNames tableNames;
        protected final TransactionManager<PgAsyncTransaction> transactionManager;

        public <E extends Event> BuilderWithEventFormat<E> withEventFormater(JacksonEventFormat<?, E> eventFormat) {
            return new BuilderWithEventFormat<>(system, pgAsyncPool, tableNames, transactionManager, eventFormat);
        }
    }

    @AllArgsConstructor
    public static class BuilderWithEventFormat<E extends Event> {
        private final ActorSystem system;
        private final PgAsyncPool pgAsyncPool;
        private final TableNames tableNames;
        private final TransactionManager<PgAsyncTransaction> transactionManager;
        private final JacksonEventFormat<?, E> eventFormat;

        public <Meta> BuilderWithMetaFormat<E, Meta> withMetaFormater(JacksonSimpleFormat<Meta> metaFormat) {
            return new BuilderWithMetaFormat<E, Meta>(system, pgAsyncPool, tableNames, transactionManager, eventFormat, metaFormat);
        }

        public BuilderWithMetaFormat<E, Tuple0> withNoMetaFormater() {
            return new BuilderWithMetaFormat<E, Tuple0>(system, pgAsyncPool, tableNames, transactionManager, eventFormat, JacksonSimpleFormat.<Tuple0>empty());
        }
    }

    @AllArgsConstructor
    public static class BuilderWithMetaFormat<E extends Event, Meta> {
        private final ActorSystem system;
        private final PgAsyncPool pgAsyncPool;
        private final TableNames tableNames;
        protected final TransactionManager<PgAsyncTransaction> transactionManager;
        private final JacksonEventFormat<?, E> eventFormat;
        private final JacksonSimpleFormat<Meta> metaFormat;

        public <Context> BuilderWithContextFormat<E, Meta, Context> withContextFormater(JacksonSimpleFormat<Context> contextFormat) {
            return new BuilderWithContextFormat<E, Meta, Context>(system, pgAsyncPool, tableNames, transactionManager, eventFormat, metaFormat, contextFormat);
        }

        public BuilderWithContextFormat<E, Meta, Tuple0> withNoContextFormater() {
            return new BuilderWithContextFormat<E, Meta, Tuple0>(system, pgAsyncPool, tableNames, transactionManager, eventFormat, metaFormat, JacksonSimpleFormat.<Tuple0>empty());
        }
    }

    @AllArgsConstructor
    public static class BuilderWithContextFormat<E extends Event, Meta, Context> {
        private final ActorSystem system;
        private final PgAsyncPool pgAsyncPool;
        private final TableNames tableNames;
        protected final TransactionManager<PgAsyncTransaction> transactionManager;
        private final JacksonEventFormat<?, E> eventFormat;
        private final JacksonSimpleFormat<Meta> metaFormat;
        private final JacksonSimpleFormat<Context> contextFormat;

        public BuilderWithKafkaSettings<E, Meta, Context> withKafkaSettings(String topic, ProducerSettings<String, EventEnvelope<E, Meta, Context>> producerSettings, Integer bufferSize) {
            return new BuilderWithKafkaSettings<>(
                    system,
                    pgAsyncPool,
                    tableNames,
                    transactionManager,
                    eventFormat,
                    metaFormat,
                    contextFormat,
                    topic,
                    producerSettings,
                    bufferSize);
        }

        public BuilderWithKafkaSettings<E, Meta, Context> withKafkaSettings(String topic, ProducerSettings<String, EventEnvelope<E, Meta, Context>> producerSettings) {
            return withKafkaSettings(topic, producerSettings, 1000);
        }
    }


    public static class BuilderWithKafkaSettings<E extends Event, Meta, Context> {
        private final ActorSystem system;
        private final PgAsyncPool pgAsyncPool;
        private final TableNames tableNames;
        protected final TransactionManager<PgAsyncTransaction> transactionManager;
        private final JacksonEventFormat<?, E> eventFormat;
        private final JacksonSimpleFormat<Meta> metaFormat;
        private final JacksonSimpleFormat<Context> contextFormat;
        private final KafkaEventPublisher<E, Meta, Context> eventPublisher;
        private final ReactivePostgresEventStore<E, Meta, Context> eventStore;

        public BuilderWithKafkaSettings(ActorSystem system, PgAsyncPool pgAsyncPool, TableNames tableNames, TransactionManager<PgAsyncTransaction> transactionManager, JacksonEventFormat<?, E> eventFormat, JacksonSimpleFormat<Meta> metaFormat, JacksonSimpleFormat<Context> contextFormat, String topic, ProducerSettings<String, EventEnvelope<E, Meta, Context>> producerSettings, Integer bufferSize) {
            this.system = system;
            this.pgAsyncPool = pgAsyncPool;
            this.tableNames = tableNames;
            this.transactionManager = transactionManager;
            this.eventFormat = eventFormat;
            this.metaFormat = metaFormat;
            this.contextFormat = contextFormat;
            this.eventPublisher = new KafkaEventPublisher<>(system, producerSettings, topic, bufferSize);
            this.eventStore = new ReactivePostgresEventStore<>(
                    system,
                    eventPublisher,
                    pgAsyncPool,
                    tableNames,
                    eventFormat,
                    metaFormat,
                    contextFormat
            );
        }
        public <S extends State<S>> BuilderWithEventHandler withEventHandler(EventHandler<S, E> eventHandler) {
            return new BuilderWithEventHandler<>(
                    system,
                    pgAsyncPool,
                    tableNames,
                    transactionManager,
                    eventFormat,
                    metaFormat,
                    contextFormat,
                    eventPublisher,
                    eventStore,
                    eventHandler
            );
        }
    }

        @AllArgsConstructor
        public static class BuilderWithEventHandler<S extends State<S>, E extends Event, Meta, Context> {
            private final ActorSystem system;
            private final PgAsyncPool pgAsyncPool;
            private final TableNames tableNames;
            protected final TransactionManager<PgAsyncTransaction> transactionManager;
            private final JacksonEventFormat<?, E> eventFormat;
            private final JacksonSimpleFormat<Meta> metaFormat;
            private final JacksonSimpleFormat<Context> contextFormat;
            private final KafkaEventPublisher<E, Meta, Context> eventPublisher;
            private final ReactivePostgresEventStore<E, Meta, Context> eventStore;
            private final EventHandler<S, E> eventHandler;

        public BuilderWithAggregateStore<S, E, Meta, Context> withAggregateStore(AggregateStore<S, String, PgAsyncTransaction> aggregateStore) {
            return new BuilderWithAggregateStore<>(
                    system,
                    pgAsyncPool,
                    tableNames,
                    transactionManager,
                    eventFormat,
                    metaFormat,
                    contextFormat,
                    eventPublisher,
                    eventStore,
                    eventHandler,
                    aggregateStore);
        }

        public BuilderWithAggregateStore<S, E, Meta, Context> withDefaultAggregateStore() {
            return new BuilderWithAggregateStore<>(
                    system,
                    pgAsyncPool,
                    tableNames,
                    transactionManager,
                    eventFormat,
                    metaFormat,
                    contextFormat,
                    eventPublisher,
                    eventStore,
                    eventHandler,
                    new DefaultAggregateStore<>(eventStore, eventHandler, system, transactionManager));
        }
    }


    @AllArgsConstructor
    public static class BuilderWithAggregateStore<S extends State<S>, E extends Event, Meta, Context> {
        private final ActorSystem system;
        private final PgAsyncPool pgAsyncPool;
        private final TableNames tableNames;
        private final TransactionManager<PgAsyncTransaction> transactionManager;
        private final JacksonEventFormat<?, E> eventFormat;
        private final JacksonSimpleFormat<Meta> metaFormat;
        private final JacksonSimpleFormat<Context> contextFormat;
        private final KafkaEventPublisher<E, Meta, Context> eventPublisher;
        private final ReactivePostgresEventStore<E, Meta, Context> eventStore;
        private final EventHandler<S, E> eventHandler;
        private final AggregateStore<S, String, PgAsyncTransaction> aggregateStore;

        public <Error, C extends Command<Meta, Context>, Message> BuilderWithCommandHandler<Error, S, C, E, Message, Meta, Context> withCommandHandler(CommandHandler<Error, S, C, E, Message, PgAsyncTransaction> commandHandler) {
            return new BuilderWithCommandHandler<>(
                    system,
                    pgAsyncPool,
                    tableNames,
                    transactionManager,
                    eventFormat,
                    metaFormat,
                    contextFormat,
                    eventPublisher,
                    eventStore,
                    aggregateStore,
                    eventHandler,
                    commandHandler
            );
        }
    }




    @AllArgsConstructor
    public static class BuilderWithCommandHandler<Error, S extends State<S>, C extends Command<Meta, Context>, E extends Event, Message, Meta, Context> {
        private final ActorSystem system;
        private final PgAsyncPool pgAsyncPool;
        private final TableNames tableNames;
        protected final TransactionManager<PgAsyncTransaction> transactionManager;
        private final JacksonEventFormat<?, E> eventFormat;
        private final JacksonSimpleFormat<Meta> metaFormat;
        private final JacksonSimpleFormat<Context> contextFormat;
        private final KafkaEventPublisher<E, Meta, Context> eventPublisher;
        private final ReactivePostgresEventStore<E, Meta, Context> eventStore;
        private final AggregateStore<S, String, PgAsyncTransaction> aggregateStore;
        private final EventHandler<S, E> eventHandler;
        private final CommandHandler<Error, S, C, E, Message, PgAsyncTransaction> commandHandler;

        public BuilderWithProjections<Error, S, C, E, Message, Meta, Context> withProjections(List<Projection<PgAsyncTransaction, E, Meta, Context>> projections) {
            return new BuilderWithProjections<>(
                    system,
                    pgAsyncPool,
                    tableNames,
                    transactionManager,
                    eventFormat,
                    metaFormat,
                    contextFormat,
                    eventPublisher,
                    eventStore,
                    aggregateStore,
                    eventHandler,
                    commandHandler,
                    projections
            );
        }

        public BuilderWithProjections<Error, S, C, E, Message, Meta, Context> withProjections(Projection<PgAsyncTransaction, E, Meta, Context>... projections) {
            return withProjections(List.of(projections));
        }

        public BuilderWithProjections<Error, S, C, E, Message, Meta, Context> withNoProjections() {
            return withProjections(List.empty());
        }
    }


    @AllArgsConstructor
    public static class BuilderWithProjections<Error, S extends State<S>, C extends Command<Meta, Context>, E extends Event, Message, Meta, Context> {
        private final ActorSystem system;
        private final PgAsyncPool pgAsyncPool;
        private final TableNames tableNames;
        private final TransactionManager<PgAsyncTransaction> transactionManager;
        private final JacksonEventFormat<?, E> eventFormat;
        private final JacksonSimpleFormat<Meta> metaFormat;
        private final JacksonSimpleFormat<Context> contextFormat;
        private final KafkaEventPublisher<E, Meta, Context> eventPublisher;
        private final ReactivePostgresEventStore<E, Meta, Context> eventStore;
        private final AggregateStore<S, String, PgAsyncTransaction> aggregateStore;
        private final EventHandler<S, E> eventHandler;
        private final CommandHandler<Error, S, C, E, Message, PgAsyncTransaction> commandHandler;
        private final List<Projection<PgAsyncTransaction, E, Meta, Context>> projections;

        public ReactivePostgresKafkaEventProcessor<Error, S, C, E, Message, Meta, Context> build() {
            return new ReactivePostgresKafkaEventProcessor<Error, S, C, E, Message, Meta, Context>(
                    new ReactivePostgresKafkaEventProcessor.PostgresKafkaEventProcessorConfig<Error, S, C, E, Message, Meta, Context>(
                            eventStore,
                            transactionManager,
                            aggregateStore,
                            commandHandler,
                            eventHandler,
                            projections,
                            eventPublisher
                    )
            );
        }

    }

}
