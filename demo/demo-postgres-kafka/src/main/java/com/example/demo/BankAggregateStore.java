package com.example.demo;

import fr.maif.concurrent.CompletionStages;
import fr.maif.eventsourcing.TransactionManager;
import fr.maif.eventsourcing.Unit;
import fr.maif.eventsourcing.vanilla.EventHandler;
import fr.maif.eventsourcing.vanilla.EventStore;
import fr.maif.reactor.eventsourcing.vanilla.DefaultAggregateStore;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class BankAggregateStore extends DefaultAggregateStore<Account, BankEvent, Unit, Unit, Connection> {

    public BankAggregateStore(EventStore<Connection, BankEvent, Unit, Unit> eventStore, EventHandler<Account, BankEvent> eventEventHandler, TransactionManager<Connection> transactionManager) {
        super(eventStore, eventEventHandler, transactionManager);
    }

    @Override
    public CompletionStage<Void> storeSnapshot(
            Connection connection,
            String id,
            Optional<Account> maybeState) {
        return CompletableFuture.runAsync(() -> maybeState.ifPresent(state -> {
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
        }));
    }

    @Override
    public CompletionStage<Optional<Account>> getAggregate(Connection connection, String entityId) {
        return CompletionStages.of(() -> {
            try(PreparedStatement statement = connection.prepareStatement("SELECT balance FROM ACCOUNTS WHERE id=?")) {
                statement.setString(1, entityId);
                ResultSet resultSet = statement.executeQuery();

                if(resultSet.next()) {
                    BigDecimal amount = resultSet.getBigDecimal("balance");

                    Account account = new Account();
                    account.id = entityId;
                    account.balance = amount;

                    return Optional.of(account);
                } else {
                    return Optional.empty();
                }
            } catch (SQLException throwable) {
                throw new RuntimeException(throwable);
            }
        });
    }
}