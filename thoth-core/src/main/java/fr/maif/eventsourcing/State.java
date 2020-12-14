package fr.maif.eventsourcing;

public interface State<S> {

    Long sequenceNum();

    S withSequenceNum(Long sequenceNum);
}
