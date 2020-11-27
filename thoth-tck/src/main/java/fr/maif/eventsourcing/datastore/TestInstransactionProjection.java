package fr.maif.eventsourcing.datastore;

import java.sql.Connection;

import akka.NotUsed;
import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.eventsourcing.Projection;
import io.vavr.Tuple0;
import io.vavr.collection.List;
import io.vavr.concurrent.Future;

public class TestInstransactionProjection implements Projection<Connection, TestEvent, Tuple0, Tuple0> {
    @Override
    public Future<Tuple0> storeProjection(Connection connection, List<EventEnvelope<TestEvent, Tuple0, Tuple0>> events) {
        // TODO écrire des trucs en base
        return null;
    }
}
