package com.example.demo;

import akka.actor.ActorSystem;
import akka.kafka.ProducerSettings;
import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.impl.TimeBasedGenerator;
import fr.maif.eventsourcing.*;
import fr.maif.eventsourcing.format.JacksonEventFormat;
import fr.maif.eventsourcing.format.JacksonSimpleFormat;
import fr.maif.eventsourcing.impl.JdbcTransactionManager;
import fr.maif.eventsourcing.impl.KafkaEventPublisher;
import fr.maif.eventsourcing.impl.PostgresEventStore;
import fr.maif.eventsourcing.impl.TableNames;
import fr.maif.kafka.JsonSerializer;
import fr.maif.kafka.KafkaSettings;
import io.vavr.Lazy;
import io.vavr.Tuple0;
import io.vavr.collection.List;
import io.vavr.concurrent.Future;
import io.vavr.control.Either;
import io.vavr.control.Option;
import org.postgresql.ds.PGSimpleDataSource;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Bank {
    private final EventProcessor<String, Account, BankCommand, BankEvent, Connection, Tuple0, Tuple0, Tuple0> eventProcessor;
    private final MeanWithdrawProjection meanWithdrawProjection;
    private static final TimeBasedGenerator UUIDgenerator = Generators.timeBasedGenerator();
    private final ActorSystem actorSystem;
    private final String schema = """
            CREATE TABLE IF NOT EXISTS ACCOUNTS (
                id varchar(100) PRIMARY KEY,
                balance money NOT NULL
            );
                
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
            );
                
            CREATE SEQUENCE if not exists bank_sequence_num;
    """;

    private DataSource dataSource() {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setDatabaseName("localhost");
        dataSource.setPassword("eventsourcing");
        dataSource.setUser("eventsourcing");
        dataSource.setDatabaseName("eventsourcing");
        dataSource.setPortNumber(5432);
        return dataSource;
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


    public Bank(ActorSystem actorSystem,
                BankCommandHandler commandHandler,
                BankEventHandler eventHandler
                ) throws SQLException {
        this.actorSystem = actorSystem;
        JacksonEventFormat<String, BankEvent> eventFormat = new BankEventFormat();
        ProducerSettings<String, EventEnvelope<BankEvent, Tuple0, Tuple0>> producerSettings = producerSettings(settings(), eventFormat);
        DataSource dataSource = dataSource();
        dataSource.getConnection().prepareStatement(schema).execute();
        ExecutorService executorService = Executors.newFixedThreadPool(5);

        this.meanWithdrawProjection = new MeanWithdrawProjection();

        this.eventProcessor = PostgresKafkaEventProcessor.create(
                actorSystem,
                eventStore(actorSystem, producerSettings, "bank", dataSource, executorService, new TableNames("bank_journal", "bank_sequence_num") ,eventFormat),
                new JdbcTransactionManager(dataSource(), Executors.newFixedThreadPool(5)),
                commandHandler,
                eventHandler,
                List.of(meanWithdrawProjection));
    }

    private EventStore<Connection, BankEvent, Tuple0, Tuple0> eventStore(
            ActorSystem actorSystem,
            ProducerSettings<String, EventEnvelope<BankEvent, Tuple0, Tuple0>> producerSettings, String topic,
            DataSource dataSource,
            ExecutorService executorService,
            TableNames tableNames,
            JacksonEventFormat<String, BankEvent> jacksonEventFormat) {
        KafkaEventPublisher<BankEvent, Tuple0, Tuple0> kafkaEventPublisher = new KafkaEventPublisher<>(actorSystem, producerSettings, topic);
        return PostgresEventStore.create(actorSystem, kafkaEventPublisher, dataSource, executorService, tableNames, jacksonEventFormat);
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

    public BigDecimal meanWithdrawValue() {
        return meanWithdrawProjection.meanWithdraw();
    }
}
