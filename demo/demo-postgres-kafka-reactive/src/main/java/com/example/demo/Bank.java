package com.example.demo;

import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.impl.TimeBasedGenerator;
import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.eventsourcing.ProcessingSuccess;
import fr.maif.eventsourcing.ReactiveEventProcessor;
import fr.maif.eventsourcing.ReactorEventProcessor;
import fr.maif.eventsourcing.TableNames;
import fr.maif.jooq.reactor.PgAsyncPool;
import fr.maif.jooq.reactor.PgAsyncTransaction;
import fr.maif.kafka.JsonFormatSerDer;
import fr.maif.reactor.kafka.KafkaSettings;
import io.vavr.Lazy;
import io.vavr.Tuple0;
import io.vavr.control.Either;
import io.vavr.control.Option;
import io.vertx.core.Vertx;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.PoolOptions;
import org.jooq.SQLDialect;
import org.jooq.impl.DefaultConfiguration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.SenderOptions;

import java.io.Closeable;
import java.io.IOException;
import java.math.BigDecimal;

import static io.vavr.API.List;
import static io.vavr.API.println;

public class Bank implements Closeable {
    private static final TimeBasedGenerator UUIDgenerator = Generators.timeBasedGenerator();
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
    private final ReactorEventProcessor<String, Account, BankCommand, BankEvent, PgAsyncTransaction, Tuple0, Tuple0, Tuple0> eventProcessor;
    private final WithdrawByMonthProjection withdrawByMonthProjection;

    public Bank(BankCommandHandler commandHandler,
                BankEventHandler eventHandler) {
        this.vertx = Vertx.vertx();
        this.pgAsyncPool = pgAsyncPool(vertx);
        this.withdrawByMonthProjection = new WithdrawByMonthProjection(pgAsyncPool);

        this.eventProcessor = ReactiveEventProcessor
                .withPgAsyncPool(pgAsyncPool)
                .withTables(tableNames())
                .withTransactionManager()
                .withEventFormater(BankEventFormat.bankEventFormat.jacksonEventFormat())
                .withNoMetaFormater()
                .withNoContextFormater()
                .withKafkaSettings("bank", senderOptions(settings()))
                .withEventHandler(eventHandler)
                .withDefaultAggregateStore()
                .withCommandHandler(commandHandler)
                .withProjections(this.withdrawByMonthProjection)
                .build();
    }

    public Mono<Void> init() {
        println("Initializing database");
        return Flux.fromIterable(List(accountTable, bankJournalTable, SEQUENCE))
                .concatMap(script -> pgAsyncPool.executeMono(d -> d.query(script)))
                .collectList()
                .doOnSuccess(__ -> println("Database initialized"))
                .doOnError(e -> {
                    println("Database initialization failed");
                    e.printStackTrace();
                })
                .flatMap(__ -> withdrawByMonthProjection.init())
                .then();
    }

    @Override
    public void close() throws IOException {
        this.pgPool.close();
        this.vertx.close();
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

        return PgAsyncPool.create(pgPool, jooqConfig);
    }

    private KafkaSettings settings() {
        return KafkaSettings.newBuilder("localhost:29092").build();
    }

    private SenderOptions<String, EventEnvelope<BankEvent, Tuple0, Tuple0>> senderOptions(KafkaSettings kafkaSettings) {
        return kafkaSettings.producerSettings(JsonFormatSerDer.of(BankEventFormat.bankEventFormat));
    }

    private TableNames tableNames() {
        return new TableNames("bank_journal", "bank_sequence_num");
    }


    public Mono<Either<String, Account>> createAccount(
            BigDecimal amount) {
        Lazy<String> lazyId = Lazy.of(() -> UUIDgenerator.generate().toString());
        return eventProcessor.processCommand(new BankCommand.OpenAccount(lazyId, amount))
                .map(res -> res.flatMap(processingResult -> processingResult.currentState.toEither("Current state is missing")));
    }

    public Mono<Either<String, Account>> withdraw(
            String account, BigDecimal amount) {
        return eventProcessor.processCommand(new BankCommand.Withdraw(account, amount))
                .map(res -> res.flatMap(processingResult -> processingResult.currentState.toEither("Current state is missing")));
    }

    public Mono<Either<String, Account>> deposit(
            String account, BigDecimal amount) {
        return eventProcessor.processCommand(new BankCommand.Deposit(account, amount))
                .map(res -> res.flatMap(processingResult -> processingResult.currentState.toEither("Current state is missing")));
    }

    public Mono<Either<String, ProcessingSuccess<Account, BankEvent, Tuple0, Tuple0, Tuple0>>> close(
            String account) {
        return eventProcessor.processCommand(new BankCommand.CloseAccount(account));
    }

    public Mono<Option<Account>> findAccountById(String id) {
        return eventProcessor.getAggregate(id);
    }

    public Mono<BigDecimal> meanWithdrawByClientAndMonth(String clientId, Integer year, String month) {
        return withdrawByMonthProjection.meanWithdrawByClientAndMonth(clientId, year, month);
    }

    public Mono<BigDecimal> meanWithdrawByClient(String clientId) {
        return withdrawByMonthProjection.meanWithdrawByClient(clientId);
    }
}
