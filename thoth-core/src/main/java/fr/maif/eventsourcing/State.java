package fr.maif.eventsourcing;

public interface State<S> {

    String entityId();

    Long sequenceNum();

    S withSequenceNum(Long sequenceNum);
}
