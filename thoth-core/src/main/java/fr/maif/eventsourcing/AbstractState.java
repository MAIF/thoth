package fr.maif.eventsourcing;

public class AbstractState<T> implements State<T> {
    protected long sequenceNum;

    @Override
    public Long sequenceNum() {
        return this.sequenceNum;
    }

    @Override
    public T withSequenceNum(Long sequenceNum) {
        this.sequenceNum = sequenceNum;
        return (T) this;
    }
}
