package fr.maif.eventsourcing;

public interface ProjectionGetter<TxCtx, E extends Event, Meta, Context> {
    Projection<TxCtx, E, Meta, Context> projection();
}
