package fr.maif.eventsourcing;

/**
 * An interface to describe an Event
 */
public interface Event {
    /**
     * The type of the event : class + name + version
     * @return
     */
    Type<?> type();

    /**
     * The id of entity
     * @return
     */
    String entityId();

    /**
     * The hash used to send to kafka and handle local order
     * @return
     */
    default String hash() {
        return entityId();
    };
}

