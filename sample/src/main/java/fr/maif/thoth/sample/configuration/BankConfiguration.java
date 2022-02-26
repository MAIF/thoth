package fr.maif.thoth.sample.configuration;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.Executors;

import javax.sql.DataSource;

import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import akka.actor.ActorSystem;
import akka.kafka.ProducerSettings;
import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.eventsourcing.EventProcessor;
import fr.maif.eventsourcing.PostgresKafkaEventProcessor;
import fr.maif.eventsourcing.format.JacksonEventFormat;
import fr.maif.eventsourcing.format.JacksonSimpleFormat;
import fr.maif.eventsourcing.impl.TableNames;
import fr.maif.kafka.JsonSerializer;
import fr.maif.kafka.KafkaSettings;
import fr.maif.thoth.sample.commands.BankCommand;
import fr.maif.thoth.sample.commands.BankCommandHandler;
import fr.maif.thoth.sample.events.BankEvent;
import fr.maif.thoth.sample.events.BankEventFormat;
import fr.maif.thoth.sample.events.BankEventHandler;
import fr.maif.thoth.sample.projections.transactional.GlobalBalanceProjection;
import fr.maif.thoth.sample.state.Account;
import io.vavr.Tuple0;

@Configuration
public class BankConfiguration {
    private final String SCHEMA = """
        CREATE TABLE IF NOT EXISTS global_balance(
            onerow_id bool PRIMARY KEY DEFAULT TRUE,
            balance money,
            CONSTRAINT onerow_uni CHECK (onerow_id)
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
        
        CREATE TABLE IF NOT EXISTS WITHDRAW_BY_MONTH(
            client_id text,
            month text,
            year smallint,
            withdraw numeric,
            count integer
        );
        CREATE UNIQUE INDEX IF NOT EXISTS WITHDRAW_BY_MONTH_UNIQUE_IDX ON WITHDRAW_BY_MONTH(client_id, month, year);
        ALTER TABLE WITHDRAW_BY_MONTH DROP CONSTRAINT IF EXISTS WITHDRAW_BY_MONTH_UNIQUE;
        ALTER TABLE WITHDRAW_BY_MONTH ADD CONSTRAINT WITHDRAW_BY_MONTH_UNIQUE UNIQUE USING INDEX WITHDRAW_BY_MONTH_UNIQUE_IDX;
        
        INSERT INTO global_balance (balance) VALUES(0::money) ON CONFLICT DO NOTHING;
    """;

    @Bean
    public DataSource dataSource(
            @Value("${db.host}") String host,
            @Value("${db.password}") String password,
            @Value("${db.user}") String user,
            @Value("${db.name}") String name,
            @Value("${db.port}") int port
    ) {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setServerName(host);
        dataSource.setPassword(password);
        dataSource.setUser(user);
        dataSource.setDatabaseName(name);
        dataSource.setPortNumber(port);

        try(final Connection connection = dataSource.getConnection();
                final PreparedStatement statement = connection.prepareStatement(SCHEMA)) {
            statement.execute();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
        return dataSource;
    }

    @Bean
    public KafkaSettings settings(
            @Value("${kafka.port}") int port,
            @Value("${kafka.host}") String host) {
        return KafkaSettings.newBuilder(host + ":" + port).build();
    }

    @Bean
    public ProducerSettings<String, EventEnvelope<BankEvent, Tuple0, Tuple0>> producerSettings(
            ActorSystem actorSystem,
            KafkaSettings kafkaSettings,
            JacksonEventFormat<String, BankEvent> eventFormat) {
        return kafkaSettings.producerSettings(actorSystem, JsonSerializer.of(
                eventFormat,
                JacksonSimpleFormat.empty(),
                JacksonSimpleFormat.empty()
                )
        );
    }

    @Bean
    public TableNames tableNames() {
        return new TableNames("bank_journal", "bank_sequence_num");
    }

    @Bean
    public JacksonEventFormat<String, BankEvent> eventFormat() {
        return new BankEventFormat();
    }

    @Bean
    public ActorSystem actorSystem() {
        return ActorSystem.create();
    }

    @Bean GlobalBalanceProjection globalBalanceProjection() {
        return new GlobalBalanceProjection();
    }

    @Bean
    public EventProcessor<String, Account, BankCommand, BankEvent, Connection, Tuple0, Tuple0, Tuple0> eventProcessor(
            ActorSystem actorSystem,
            GlobalBalanceProjection balanceProjection,
            DataSource dataSource,
            JacksonEventFormat<String, BankEvent> eventFormat,
            TableNames tableNames,
            ProducerSettings<String, EventEnvelope<BankEvent, Tuple0, Tuple0>> producerSettings) throws SQLException {
        var processor = PostgresKafkaEventProcessor.withActorSystem(actorSystem)
                .withDataSource(dataSource)
                .withTables(tableNames)
                .withTransactionManager(Executors.newFixedThreadPool(5))
                .withEventFormater(eventFormat)
                .withNoMetaFormater()
                .withNoContextFormater()
                .withKafkaSettings("bank", producerSettings)
                .withEventHandler(new BankEventHandler())
                .withDefaultAggregateStore()
                .withCommandHandler(new BankCommandHandler())
                .withProjections(balanceProjection)
                .build();

        balanceProjection.initialize(dataSource.getConnection(), processor.eventStore(), actorSystem);

        return processor;
    }
}
