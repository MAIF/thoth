package com.example.demo;

import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.eventsourcing.Projection;
import fr.maif.jooq.PgAsyncPool;
import fr.maif.jooq.PgAsyncTransaction;
import io.vavr.Tuple;
import io.vavr.Tuple0;
import io.vavr.collection.List;
import io.vavr.concurrent.Future;
import org.jooq.DSLContext;

import java.math.BigDecimal;

import static io.vavr.PartialFunction.unlift;
import static org.jooq.impl.DSL.val;
import static io.vavr.API.*;

public class WithdrawByMonthProjection implements Projection<PgAsyncTransaction, BankEvent, Tuple0, Tuple0> {

    private final PgAsyncPool pgAsyncPool;

    public WithdrawByMonthProjection(PgAsyncPool pgAsyncPool) {
        this.pgAsyncPool = pgAsyncPool;
    }

    @Override
    public Future<Tuple0> storeProjection(PgAsyncTransaction connection, List<EventEnvelope<BankEvent, Tuple0, Tuple0>> envelopes) {
        return connection.executeBatch(dsl ->
                envelopes
                        // Keep only MoneyWithdrawn events
                        .collect(unlift(eventEnvelope ->
                            Match(eventEnvelope.event).option(
                                Case(BankEvent.$MoneyWithdrawn(), e -> Tuple(eventEnvelope, e))
                            )
                        ))
                        // Store withdraw by month
                        .map(t -> dsl.query("""
                                        insert into withdraw_by_month (client_id, month, year, withdraw) values ({0}, {1}, {2}, {3}) 
                                        on conflict on constraint WITHDRAW_BY_MONTH_UNIQUE 
                                        do update set withdraw = EXCLUDED.withdraw + {3}
                                    """,
                                    val(t._2.entityId()),
                                    val(t._1.emissionDate.getMonth().name()),
                                    val(t._1.emissionDate.getYear()),
                                    val(t._2.amount)
                        ))
            ).map(__ -> Tuple.empty());
    }

    public Future<BigDecimal> meanWithdrawByClientAndMonth(String clientId, Integer year, String month) {
        return pgAsyncPool.query(dsl -> dsl.resultQuery(
                """
                    select withdraw 
                    from withdraw_by_month 
                    where  client_id = {0} and year = {1} and month = {2}                   
                    """,
                val(clientId),
                val(year),
                val(month))
        ).map(r -> r.head().get(0, BigDecimal.class));
    }

    public Future<BigDecimal> meanWithdrawByClient(String clientId) {
        return pgAsyncPool.query(dsl -> dsl
                .resultQuery(
                """
                    select client_id, sum(withdraw) as sum
                    from withdraw_by_month 
                    where  client_id = {0} 
                    group by client_id                  
                    """, val(clientId)
                )
        ).map(r -> r.head().get("sum", BigDecimal.class));
    }


    public Future<Integer> init() {
        return this.pgAsyncPool.execute(dsl -> dsl.query("""
                 CREATE TABLE IF NOT EXISTS WITHDRAW_BY_MONTH(
                     client_id text, 
                     month text,
                     year smallint, 
                     withdraw numeric               
                 )
                """))
                .flatMap(__ ->
                        pgAsyncPool.execute(dsl -> dsl.query("""
                        CREATE UNIQUE INDEX IF NOT EXISTS WITHDRAW_BY_MONTH_UNIQUE_IDX ON WITHDRAW_BY_MONTH(client_id, month, year);    
                    """))
                )
                .flatMap(__ ->
                        pgAsyncPool.execute(dsl -> dsl.query("""
                        ALTER TABLE WITHDRAW_BY_MONTH
                        DROP CONSTRAINT IF EXISTS WITHDRAW_BY_MONTH_UNIQUE;    
                    """))
                )
                .flatMap(__ ->
                        pgAsyncPool.execute(dsl -> dsl.query("""
                        ALTER TABLE WITHDRAW_BY_MONTH
                        ADD CONSTRAINT WITHDRAW_BY_MONTH_UNIQUE 
                        UNIQUE USING INDEX WITHDRAW_BY_MONTH_UNIQUE_IDX;    
                    """))
                );
    }
}
