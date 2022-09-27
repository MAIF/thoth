package com.example.demo;

import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.impl.TimeBasedGenerator;
import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.eventsourcing.EventProcessorImpl;
import fr.maif.eventsourcing.PostgresKafkaEventProcessor;
import fr.maif.eventsourcing.ProcessingSuccess;
import fr.maif.eventsourcing.format.JacksonEventFormat;
import fr.maif.eventsourcing.format.JacksonSimpleFormat;
import fr.maif.eventsourcing.impl.JdbcTransactionManager;
import fr.maif.eventsourcing.impl.PostgresEventStore;
import fr.maif.reactor.eventsourcing.ReactorKafkaEventPublisher;
import fr.maif.eventsourcing.impl.TableNames;
import fr.maif.kafka.JsonSerializer;
import fr.maif.reactor.kafka.KafkaSettings;
import io.vavr.Lazy;
import io.vavr.Tuple0;
import io.vavr.collection.List;
import io.vavr.control.Either;
import io.vavr.control.Option;
import org.postgresql.ds.PGSimpleDataSource;
import reactor.kafka.sender.SenderOptions;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Bank {
    private final EventProcessorImpl<String, Account, BankCommand, BankEvent, Connection, List<String>, Tuple0, Tuple0> eventProcessor;
    private final MeanWithdrawProjection meanWithdrawProjection;
    private static final TimeBasedGenerator UUIDgenerator = Generators.timeBasedGenerator();
    private final String SCHEMA = """
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

    private SenderOptions<String, EventEnvelope<BankEvent, Tuple0, Tuple0>> producerSettings(
            KafkaSettings kafkaSettings,
            JacksonEventFormat<String, BankEvent> eventFormat) {
        return kafkaSettings.producerSettings(JsonSerializer.of(
                eventFormat,
                JacksonSimpleFormat.empty(),
                JacksonSimpleFormat.empty()
            )
        );
    }

    private TableNames tableNames() {
        return new TableNames("bank_journal", "bank_sequence_num");
    }


    public Bank(BankCommandHandler commandHandler,
                BankEventHandler eventHandler
                ) throws SQLException {
        String topic = "bank";
        JacksonEventFormat<String, BankEvent> eventFormat = new BankEventFormat();
        SenderOptions<String, EventEnvelope<BankEvent, Tuple0, Tuple0>> producerSettings = producerSettings(settings(), eventFormat);
        DataSource dataSource = dataSource();
        dataSource.getConnection().prepareStatement(SCHEMA).execute();
        TableNames tableNames = tableNames();

        this.meanWithdrawProjection = new MeanWithdrawProjection();

        ExecutorService executorService = Executors.newFixedThreadPool(5);
        JdbcTransactionManager transactionManager = new JdbcTransactionManager(dataSource(), executorService);

        this.eventProcessor = PostgresKafkaEventProcessor
                .withDataSource(dataSource())
                .withTables(tableNames)
                .withTransactionManager(transactionManager, executorService)
                .withEventFormater(eventFormat)
                .withNoMetaFormater()
                .withNoContextFormater()
                .withKafkaSettings(topic, producerSettings)
                .withEventHandler(eventHandler)
                .withAggregateStore(builder -> new BankAggregateStore(
                            builder.eventStore,
                            builder.eventHandler,
                            builder.transactionManager
                    ))
                .withCommandHandler(commandHandler)
                .withProjections(meanWithdrawProjection)
                .build();
    }

    private ReactorKafkaEventPublisher<BankEvent, Tuple0, Tuple0> kafkaEventPublisher(
            SenderOptions<String, EventEnvelope<BankEvent, Tuple0, Tuple0>> producerSettings,
            String topic) {
        return new ReactorKafkaEventPublisher<>(producerSettings, topic);
    }

    private PostgresEventStore<BankEvent, Tuple0, Tuple0> eventStore(
            ReactorKafkaEventPublisher<BankEvent, Tuple0, Tuple0> kafkaEventPublisher,
            DataSource dataSource,
            ExecutorService executorService,
            TableNames tableNames,
            JacksonEventFormat<String, BankEvent> jacksonEventFormat) {
        return PostgresEventStore.create(kafkaEventPublisher, dataSource, executorService, tableNames, jacksonEventFormat);
    }


    public CompletionStage<Either<String, ProcessingSuccess<Account, BankEvent, Tuple0, Tuple0, List<String>>>> createAccount(
            BigDecimal amount) {
        Lazy<String> lazyId = Lazy.of(() -> UUIDgenerator.generate().toString());
        return eventProcessor.processCommand(new BankCommand.OpenAccount(lazyId, amount));
    }

    public CompletionStage<Either<String, ProcessingSuccess<Account, BankEvent, Tuple0, Tuple0, List<String>>>> withdraw(
            String account, BigDecimal amount) {
        return eventProcessor.processCommand(new BankCommand.Withdraw(account, amount));
    }

    public CompletionStage<Either<String, ProcessingSuccess<Account, BankEvent, Tuple0, Tuple0, List<String>>>> deposit(
            String account, BigDecimal amount) {
        return eventProcessor.processCommand(new BankCommand.Deposit(account, amount));
    }

    public CompletionStage<Either<String, ProcessingSuccess<Account, BankEvent, Tuple0, Tuple0, List<String>>>> close(
            String account) {
        return eventProcessor.processCommand(new BankCommand.CloseAccount(account));
    }

    public CompletionStage<Option<Account>> findAccountById(String id) {
        return eventProcessor.getAggregate(id);
    }

    public BigDecimal meanWithdrawValue() {
        return meanWithdrawProjection.meanWithdraw();
    }
}
