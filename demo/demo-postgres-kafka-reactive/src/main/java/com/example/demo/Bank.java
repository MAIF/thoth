package com.example.demo;

import akka.actor.ActorSystem;
import akka.kafka.ProducerSettings;
import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.impl.TimeBasedGenerator;
import fr.maif.eventsourcing.*;
import fr.maif.eventsourcing.format.JacksonEventFormat;
import fr.maif.eventsourcing.format.JacksonSimpleFormat;
import fr.maif.eventsourcing.impl.KafkaEventPublisher;
import fr.maif.jooq.PgAsyncPool;
import fr.maif.jooq.PgAsyncTransaction;
import fr.maif.jooq.reactive.ReactivePgAsyncPool;
import fr.maif.kafka.JsonSerializer;
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

import java.math.BigDecimal;

import static io.vavr.API.*;

public class Bank {
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

    public Bank(ActorSystem actorSystem,
                BankCommandHandler commandHandler,
                BankEventHandler eventHandler) {
        this.actorSystem = actorSystem;
        this.pgAsyncPool = pgAsyncPool();
        this.eventProcessor = ReactivePostgresKafkaEventProcessor.create(
                actorSystem,
                eventStore(actorSystem, pgAsyncPool),
                new ReactiveTransactionManager(pgAsyncPool),
                commandHandler,
                eventHandler,
                List.empty());
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

    private EventStore<PgAsyncTransaction, BankEvent, Tuple0, Tuple0> eventStore(ActorSystem actorSystem, PgAsyncPool dataSource) {
        BankEventFormat eventFormat = new BankEventFormat();
        KafkaEventPublisher<BankEvent, Tuple0, Tuple0> kafkaEventPublisher = new KafkaEventPublisher<>(actorSystem, producerSettings(settings(), eventFormat), "bank");
        return ReactivePostgresEventStore.create(actorSystem, kafkaEventPublisher, dataSource, tableNames(), eventFormat, JacksonSimpleFormat.empty(), JacksonSimpleFormat.empty());
    }

    private PgAsyncPool pgAsyncPool() {
        DefaultConfiguration jooqConfig = new DefaultConfiguration();
        jooqConfig.setSQLDialect(SQLDialect.POSTGRES);

        PgConnectOptions options = new PgConnectOptions()
                .setPort(5432)
                .setHost("localhost")
                .setDatabase("eventsourcing")
                .setUser("eventsourcing")
                .setPassword("eventsourcing");
        PoolOptions poolOptions = new PoolOptions().setMaxSize(50);
        Vertx vertx = Vertx.vertx();
        PgPool client = PgPool.pool(vertx, options, poolOptions);

        return new ReactivePgAsyncPool(client, jooqConfig);
    }

    private KafkaSettings settings() {
        return KafkaSettings.newBuilder("localhost:29092").build();
    }

    private ProducerSettings<String, EventEnvelope<BankEvent, Tuple0, Tuple0>> producerSettings(
            KafkaSettings kafkaSettings,
            JacksonEventFormat<String, BankEvent> eventFormat) {
        return kafkaSettings.producerSettings(actorSystem, JsonSerializer.of(
                eventFormat,
                JacksonSimpleFormat.empty(),
                JacksonSimpleFormat.empty()
                )
        );
    }

    private TableNames tableNames() {
        return new TableNames("bank_journal", "bank_sequence_num");
    }



    public Future<Either<String, ProcessingSuccess<Account, BankEvent, Tuple0, Tuple0, Tuple0>>> createAccount(
            BigDecimal amount) {
        Lazy<String> lazyId = Lazy.of(() -> UUIDgenerator.generate().toString());
        return eventProcessor.processCommand(new BankCommand.OpenAccount(lazyId, amount));
    }

    public Future<Either<String, ProcessingSuccess<Account, BankEvent, Tuple0, Tuple0, Tuple0>>> withdraw(
            String account, BigDecimal amount) {
        return eventProcessor.processCommand(new BankCommand.Withdraw(account, amount));
    }

    public Future<Either<String, ProcessingSuccess<Account, BankEvent, Tuple0, Tuple0, Tuple0>>> deposit(
            String account, BigDecimal amount) {
        return eventProcessor.processCommand(new BankCommand.Deposit(account, amount));
    }

    public Future<Either<String, ProcessingSuccess<Account, BankEvent, Tuple0, Tuple0, Tuple0>>> close(
            String account) {
        return eventProcessor.processCommand(new BankCommand.CloseAccount(account));
    }

    public Future<Option<Account>> findAccountById(String id) {
        return eventProcessor.getAggregate(id);
    }
}
