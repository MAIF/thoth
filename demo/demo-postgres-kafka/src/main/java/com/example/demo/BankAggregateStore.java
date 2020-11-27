package com.example.demo;

import akka.actor.ActorSystem;
import akka.stream.Materializer;
import fr.maif.eventsourcing.EventHandler;
import fr.maif.eventsourcing.EventStore;
import fr.maif.eventsourcing.TransactionManager;
import fr.maif.eventsourcing.impl.DefaultAggregateStore;
import io.vavr.Tuple;
import io.vavr.Tuple0;
import io.vavr.concurrent.Future;
import io.vavr.control.Option;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class BankAggregateStore extends DefaultAggregateStore<Account, BankEvent, Tuple0, Tuple0, Connection> {

    public BankAggregateStore(EventStore<Connection, BankEvent, Tuple0, Tuple0> eventStore, EventHandler<Account, BankEvent> eventEventHandler, ActorSystem system, TransactionManager<Connection> transactionManager) {
        super(eventStore, eventEventHandler, system, transactionManager);
    }

    public BankAggregateStore(EventStore<Connection, BankEvent, Tuple0, Tuple0> eventStore, EventHandler<Account, BankEvent> eventEventHandler, Materializer materializer, TransactionManager<Connection> transactionManager) {
        super(eventStore, eventEventHandler, materializer, transactionManager);
    }

    @Override
    public Future<Tuple0> storeSnapshot(
            Connection connection,
            String id,
            Option<Account> maybeState) {
        return Future.of(() -> {

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
    public Future<Option<Account>> getAggregate(Connection connection, String entityId) {
        return Future.of(() -> {
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
        });
    }
}