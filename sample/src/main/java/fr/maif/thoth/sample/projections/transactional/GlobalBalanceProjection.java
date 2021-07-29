package fr.maif.thoth.sample.projections.transactional;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;

import akka.Done;
import akka.actor.ActorSystem;
import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.eventsourcing.EventStore;
import fr.maif.eventsourcing.Projection;
import fr.maif.thoth.sample.events.BankEvent;
import io.vavr.Tuple;
import io.vavr.Tuple0;
import io.vavr.collection.List;
import io.vavr.concurrent.Future;

public class GlobalBalanceProjection implements Projection<Connection, BankEvent, Tuple0, Tuple0> {

    public CompletableFuture<Done> initialize(Connection connection, EventStore<Connection, BankEvent, Tuple0, Tuple0> eventStore, ActorSystem actorSystem) {
        return eventStore.loadAllEvents()
                .map(enveloppe -> enveloppe.event)
                .filter(event -> event instanceof BankEvent.MoneyDeposited || event instanceof BankEvent.MoneyWithdrawn)
                .mapAsync(1, event ->
                    CompletableFuture.supplyAsync(() -> {
                        try {
                            if(event instanceof BankEvent.MoneyDeposited deposit) {
                                String statement = "UPDATE global_balance SET balance=balance+?::money";
                                try(PreparedStatement preparedStatement = connection.prepareStatement(statement)) {
                                    preparedStatement.setBigDecimal(1, deposit.amount);
                                    preparedStatement.execute();
                                }
                            } else if(event instanceof BankEvent.MoneyWithdrawn withdraw) {
                                String statement = "UPDATE global_balance SET balance=balance-?::money";
                                try(PreparedStatement preparedStatement = connection.prepareStatement(statement)) {
                                    preparedStatement.setBigDecimal(1, withdraw.amount);
                                    preparedStatement.execute();
                                }
                            }
                            return Tuple.empty();
                        } catch(SQLException ex) {
                            throw new RuntimeException(ex);
                        }
                    })
                ).run(actorSystem).toCompletableFuture();
    }

    @Override
    public Future<Tuple0> storeProjection(Connection connection, List<EventEnvelope<BankEvent, Tuple0, Tuple0>> events) {
        return Future.of(() -> {
            try(PreparedStatement incrementStatement = connection.prepareStatement("UPDATE global_balance SET balance=balance+?::money");
                    PreparedStatement decrementStatement = connection.prepareStatement("UPDATE global_balance SET balance=balance-?::money")) {
                for(var envelope: events) {
                    final BankEvent event = envelope.event;

                    if(event instanceof BankEvent.MoneyDeposited deposit) {
                        incrementStatement.setBigDecimal(1, deposit.amount);

                        incrementStatement.addBatch();
                    } else if(event instanceof BankEvent.MoneyWithdrawn withdraw) {
                        decrementStatement.setBigDecimal(1, withdraw.amount);

                        decrementStatement.addBatch();
                    }
                }

                incrementStatement.executeBatch();
                decrementStatement.executeBatch();

                return Tuple.empty();
            } catch (SQLException ex) {
                throw new RuntimeException("Failed to update projection", ex);
            }
        });
    }
}
