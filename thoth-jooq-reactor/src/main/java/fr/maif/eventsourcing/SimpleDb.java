package fr.maif.eventsourcing;

import org.jooq.DSLContext;
import org.jooq.Query;
import org.jooq.Record1;
import org.jooq.ResultQuery;

import java.util.concurrent.CompletionStage;
import java.util.function.Function;

public interface SimpleDb<Tx> {

    CompletionStage<Long> count(Function<DSLContext, ? extends ResultQuery<Record1<Long>>> queryFunction);

    CompletionStage<Integer> execute(Function<DSLContext, ? extends Query> queryFunction);
    CompletionStage<Tx> begin();
}
