package fr.maif.eventsourcing;

import fr.maif.jooq.PgAsyncPool;

public class ReactiveEventProcessor {

    public static ReactivePostgresKafkaEventProcessorBuilder.BuilderWithPool withPgAsyncPool(PgAsyncPool pgAsyncPool) {
        return new ReactivePostgresKafkaEventProcessorBuilder.BuilderWithPool(pgAsyncPool);
    }

    public static ReactorPostgresKafkaEventProcessorBuilder.BuilderWithPool withPgAsyncPool(fr.maif.jooq.reactor.PgAsyncPool pgAsyncPool) {
        return new ReactorPostgresKafkaEventProcessorBuilder.BuilderWithPool(pgAsyncPool);
    }

}
