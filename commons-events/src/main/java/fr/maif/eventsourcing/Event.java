package fr.maif.eventsourcing;

public interface Event {
    Type<?> type();
    String entityId();
    default String hash() {
        return entityId();
    };
}

