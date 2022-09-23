package com.example.demo;

import fr.maif.concurrent.CompletionStages;
import fr.maif.eventsourcing.EventHandler;
import fr.maif.eventsourcing.EventStore;
import fr.maif.eventsourcing.TransactionManager;
import fr.maif.eventsourcing.impl.DefaultAggregateStore;
import io.vavr.Tuple;
import io.vavr.Tuple0;
import io.vavr.control.Option;
import io.vavr.control.Try;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.CompletionStage;

public class BankAggregateStore extends DefaultAggregateStore<Account, BankEvent, Tuple0, Tuple0, Connection> {

    public BankAggregateStore(EventStore<Connection, BankEvent, Tuple0, Tuple0> eventStore, EventHandler<Account, BankEvent> eventEventHandler, TransactionManager<Connection> transactionManager) {
        super(eventStore, eventEventHandler, transactionManager);
    }

    @Override
    public CompletionStage<Tuple0> storeSnapshot(
            Connection connection,
            String id,
            Option<Account> maybeState) {
        return CompletionStages.of(() -> {

            maybeState.peek(state -> {
                try {
                    PreparedStatement statement = connection.prepareStatement("""
                        INSERT INTO ACCOUNTS(ID, BALANCE) VALUES(?, ?)
                        ON CONFLICT (id) DO UPDATE SET balance = ?
                    """);
                    statement.setString(1, id);
                    statement.setBigDecimal(2, state.balance);
                    statement.setBigDecimal(3, state.balance);
                    statement.execute();
                } catch (SQLException throwable) {
                    throw new RuntimeException(throwable);
                }
            });

            return Tuple.empty();
        });
    }

    @Override
    public CompletionStage<Option<Account>> getAggregate(Connection connection, String entityId) {
        return CompletionStages.fromTry(() -> Try.of(() -> {
                PreparedStatement statement = connection.prepareStatement("SELECT balance FROM ACCOUNTS WHERE id=?");
                statement.setString(1, entityId);
                ResultSet resultSet = statement.executeQuery();

                if(resultSet.next()) {
                    BigDecimal amount = resultSet.getBigDecimal("balance");

                    Account account = new Account();
                    account.id = entityId;
                    account.balance = amount;

                    return Option.some(account);
                } else {
                    return Option.none();
                }
        }));
    }
}