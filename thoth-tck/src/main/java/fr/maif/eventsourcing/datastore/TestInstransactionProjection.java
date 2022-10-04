package fr.maif.eventsourcing.datastore;

import fr.maif.concurrent.CompletionStages;
import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.eventsourcing.Projection;
import io.vavr.Tuple0;
import io.vavr.collection.List;

import java.sql.Connection;
import java.util.concurrent.CompletionStage;

public class TestInstransactionProjection implements Projection<Connection, TestEvent, Tuple0, Tuple0> {
    @Override
    public CompletionStage<Tuple0> storeProjection(Connection connection, List<EventEnvelope<TestEvent, Tuple0, Tuple0>> events) {
        // TODO écrire des trucs en base
        return CompletionStages.empty();
    }
}
