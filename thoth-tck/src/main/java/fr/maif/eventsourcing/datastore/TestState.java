package fr.maif.eventsourcing.datastore;

import fr.maif.eventsourcing.AbstractState;

public class TestState extends AbstractState<TestState> {
    public final String id;
    public final int count;


    public TestState(String id, int count) {
        this.id = id;
        this.count = count;
    }
}
