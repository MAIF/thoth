package com.example.demo;

import akka.actor.ActorSystem;
import akka.kafka.ProducerSettings;
import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.impl.TimeBasedGenerator;
import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.eventsourcing.EventProcessor;
import fr.maif.eventsourcing.ProcessingSuccess;
import fr.maif.eventsourcing.ReactivePostgresEventStore;
import fr.maif.eventsourcing.ReactivePostgresKafkaEventProcessor;
import fr.maif.eventsourcing.ReactiveTransactionManager;
import fr.maif.eventsourcing.TableNames;
import fr.maif.eventsourcing.format.JacksonSimpleFormat;
import fr.maif.eventsourcing.impl.DefaultAggregateStore;
import fr.maif.eventsourcing.impl.KafkaEventPublisher;
import fr.maif.jooq.PgAsyncPool;
import fr.maif.jooq.PgAsyncTransaction;
import fr.maif.jooq.reactive.ReactivePgAsyncPool;
import fr.maif.kafka.JsonFormatSerDer;
import fr.maif.kafka.KafkaSettings;
import io.vavr.Lazy;
import io.vavr.Tuple0;
import io.vavr.collection.List;
import io.vavr.collection.Seq;
import io.vavr.concurrent.Future;
import io.vavr.control.Either;
import io.vavr.control.Option;
import io.vertx.core.Vertx;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.PoolOptions;
import org.jooq.SQLDialect;
import org.jooq.impl.DefaultConfiguration;

import java.io.Closeable;
import java.io.IOException;
import java.math.BigDecimal;

import static io.vavr.API.List;
import static io.vavr.API.println;

public class Bank implements Closeable {
    private final EventProcessor<String, Account, BankCommand, BankEvent, PgAsyncTransaction, Tuple0, Tuple0, Tuple0> eventProcessor;
    private static final TimeBasedGenerator UUIDgenerator = Generators.timeBasedGenerator();
    private final ActorSystem actorSystem;
    private final String accountTable = """
            CREATE TABLE IF NOT EXISTS ACCOUNTS (
                id varchar(100) PRIMARY KEY,
                balance money NOT NULL
            );""";
    private final String bankJournalTable = """
                
            CREATE TABLE IF NOT EXISTS bank_journal (
              id UUID primary key,
              entity_id varchar(100) not null,
              sequence_num bigint not null,
              event_type varchar(100) not null,
              version int not null,
              transaction_id varchar(100) not null,
              event jsonb not null,
              metadata jsonb,
              context jsonb,
              total_message_in_transaction int default 1,
              num_message_in_transaction int default 1,
              emission_date timestamp not null default now(),
              user_id varchar(100),
              system_id varchar(100),
              published boolean default false,
              UNIQUE (entity_id, sequence_num)
            );""";
    private final String SEQUENCE = """
                    CREATE SEQUENCE if not exists bank_sequence_num;
            """;
    private final PgAsyncPool pgAsyncPool;
    private final Vertx vertx;
    private PgPool pgPool;

    public Bank(ActorSystem actorSystem,
                BankCommandHandler commandHandler,
                BankEventHandler eventHandler) {
        this.actorSystem = actorSystem;
        this.vertx = Vertx.vertx();
        this.pgAsyncPool = pgAsyncPool(vertx);

        var eventStore = eventStore(actorSystem, pgAsyncPool);
        var transactionManager = new ReactiveTransactionManager(pgAsyncPool);
        var producerSettings = producerSettings(settings());
        this.eventProcessor = new ReactivePostgresKafkaEventProcessor<>(
                new ReactivePostgresKafkaEventProcessor.PostgresKafkaEventProcessorConfig<>(
                        eventStore,
                        transactionManager,
                        new DefaultAggregateStore<>(eventStore, eventHandler, actorSystem, transactionManager),
                        commandHandler,
                        eventHandler,
                        List.empty(),
                        new KafkaEventPublisher<>(actorSystem, producerSettings, "bank")
                )
        );
    }

    public Future<Seq<Integer>> init() {
        println("Initializing database");
        return Future.traverse(List(accountTable, bankJournalTable, SEQUENCE), script -> pgAsyncPool.execute(d -> d.query(script)))
                .onSuccess(__ -> println("Database initialized"))
                .onFailure(e -> {
                    println("Database initialization failed");
                    e.printStackTrace();
                });
    }

    @Override
    public void close() throws IOException {
        this.pgPool.close();
        this.vertx.close();
    }

    private ReactivePostgresEventStore<BankEvent, Tuple0, Tuple0> eventStore(ActorSystem actorSystem, PgAsyncPool dataSource) {
        KafkaEventPublisher<BankEvent, Tuple0, Tuple0> kafkaEventPublisher = new KafkaEventPublisher<>(actorSystem, producerSettings(settings()), "bank");
        return ReactivePostgresEventStore.create(actorSystem, kafkaEventPublisher, dataSource, tableNames(), BankEventFormat.bankEventFormat.jacksonEventFormat(), JacksonSimpleFormat.empty(), JacksonSimpleFormat.empty());
    }

    private PgAsyncPool pgAsyncPool(Vertx vertx) {
        DefaultConfiguration jooqConfig = new DefaultConfiguration();
        jooqConfig.setSQLDialect(SQLDialect.POSTGRES);

        PgConnectOptions options = new PgConnectOptions()
                .setPort(5432)
                .setHost("localhost")
                .setDatabase("eventsourcing")
                .setUser("eventsourcing")
                .setPassword("eventsourcing");
        PoolOptions poolOptions = new PoolOptions().setMaxSize(50);
        pgPool = PgPool.pool(vertx, options, poolOptions);

        return new ReactivePgAsyncPool(pgPool, jooqConfig);
    }

    private KafkaSettings settings() {
        return KafkaSettings.newBuilder("localhost:29092").build();
    }

    private ProducerSettings<String, EventEnvelope<BankEvent, Tuple0, Tuple0>> producerSettings(KafkaSettings kafkaSettings) {
        return kafkaSettings.producerSettings(actorSystem, JsonFormatSerDer.of(BankEventFormat.bankEventFormat));
    }

    private TableNames tableNames() {
        return new TableNames("bank_journal", "bank_sequence_num");
    }



    public Future<Either<String, Account>> createAccount(
            BigDecimal amount) {
        Lazy<String> lazyId = Lazy.of(() -> UUIDgenerator.generate().toString());
        return eventProcessor.processCommand(new BankCommand.OpenAccount(lazyId, amount))
                .map(res -> res.flatMap(processingResult -> processingResult.currentState.toEither("Current state is missing")));
    }

    public Future<Either<String, Account>> withdraw(
            String account, BigDecimal amount) {
        return eventProcessor.processCommand(new BankCommand.Withdraw(account, amount))
                .map(res -> res.flatMap(processingResult -> processingResult.currentState.toEither("Current state is missing")));
    }

    public Future<Either<String, Account>> deposit(
            String account, BigDecimal amount) {
        return eventProcessor.processCommand(new BankCommand.Deposit(account, amount))
                .map(res -> res.flatMap(processingResult -> processingResult.currentState.toEither("Current state is missing")));
    }

    public Future<Either<String, ProcessingSuccess<Account, BankEvent, Tuple0, Tuple0, Tuple0>>> close(
            String account) {
        return eventProcessor.processCommand(new BankCommand.CloseAccount(account));
    }

    public Future<Option<Account>> findAccountById(String id) {
        return eventProcessor.getAggregate(id);
    }
}
