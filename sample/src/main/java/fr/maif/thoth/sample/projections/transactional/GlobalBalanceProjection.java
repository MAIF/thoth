package fr.maif.thoth.sample.projections.transactional;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.eventsourcing.Projection;
import fr.maif.thoth.sample.events.BankEvent;
import io.vavr.Tuple0;
import io.vavr.collection.List;
import io.vavr.concurrent.Future;

public class GlobalBalanceProjection implements Projection<Connection, BankEvent, Tuple0, Tuple0> {

    @Override
    public Future<Tuple0> storeProjection(Connection connection, List<EventEnvelope<BankEvent, Tuple0, Tuple0>> events) {
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
        } catch (SQLException ex) {
            throw new RuntimeException("Failed to update projection", ex);
        }

        return Future.successful(Tuple0.instance());
    }
}
