package fr.maif.eventsourcing;

import org.jooq.DSLContext;
import org.jooq.Query;

import java.util.concurrent.CompletionStage;
import java.util.function.Function;

public interface SimpleDb<Tx> {

    CompletionStage<Integer> execute(Function<DSLContext, ? extends Query> queryFunction);
    CompletionStage<Tx> begin();
}
