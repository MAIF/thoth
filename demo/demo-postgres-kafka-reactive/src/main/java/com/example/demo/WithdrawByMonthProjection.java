package com.example.demo;

import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.eventsourcing.ReactorProjection;
import fr.maif.jooq.reactor.PgAsyncPool;
import fr.maif.jooq.reactor.PgAsyncTransaction;
import io.vavr.Tuple;
import io.vavr.Tuple0;
import io.vavr.collection.List;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;

import static io.vavr.API.Case;
import static io.vavr.API.Match;
import static io.vavr.API.Tuple;
import static io.vavr.PartialFunction.unlift;
import static org.jooq.impl.DSL.val;

public class WithdrawByMonthProjection implements ReactorProjection<PgAsyncTransaction, BankEvent, Tuple0, Tuple0> {

    private final PgAsyncPool pgAsyncPool;

    public WithdrawByMonthProjection(PgAsyncPool pgAsyncPool) {
        this.pgAsyncPool = pgAsyncPool;
    }

    @Override
    public Mono<Tuple0> storeProjection(PgAsyncTransaction connection, List<EventEnvelope<BankEvent, Tuple0, Tuple0>> envelopes) {
        return connection.executeBatchMono(dsl ->
                envelopes
                        // Keep only MoneyWithdrawn events
                        .collect(unlift(eventEnvelope ->
                                Match(eventEnvelope.event).option(
                                        Case(BankEvent.$MoneyWithdrawn(), e -> Tuple(eventEnvelope, e))
                                )
                        ))
                        // Store withdraw by month
                        .map(t -> dsl.query("""
                                        insert into withdraw_by_month (client_id, month, year, withdraw, count) values ({0}, {1}, {2}, {3}, 1)
                                        on conflict on constraint WITHDRAW_BY_MONTH_UNIQUE
                                        do update set withdraw = withdraw_by_month.withdraw + EXCLUDED.withdraw, count=withdraw_by_month.count + 1
                                    """,
                                val(t._2.entityId()),
                                val(t._1.emissionDate.getMonth().name()),
                                val(t._1.emissionDate.getYear()),
                                val(t._2.amount)
                        ))
        ).thenReturn(Tuple.empty());
    }

    public Mono<BigDecimal> meanWithdrawByClientAndMonth(String clientId, Integer year, String month) {
        return pgAsyncPool.queryMono(dsl -> dsl.resultQuery(
                """
                    select round(withdraw / count::decimal, 2) 
                    from withdraw_by_month 
                    where  client_id = {0} and year = {1} and month = {2}                   
                    """,
                val(clientId),
                val(year),
                val(month))
        ).map(r -> r.head().get(0, BigDecimal.class));
    }

    public Mono<BigDecimal> meanWithdrawByClient(String clientId) {
        return pgAsyncPool.queryMono(dsl -> dsl
                .resultQuery(
                        """
                            select round(sum(withdraw) / sum(count)::decimal, 2) as sum
                            from withdraw_by_month 
                            where  client_id = {0}
                            """, val(clientId)
                )
        ).map(r -> r.head().get("sum", BigDecimal.class));
    }

    public Mono<Integer> init() {
        return this.pgAsyncPool.executeMono(dsl -> dsl.query("""
                 CREATE TABLE IF NOT EXISTS WITHDRAW_BY_MONTH(
                    client_id text,
                    month text,
                    year smallint,
                    withdraw numeric,
                    count integer            
                 )
                """))
                .flatMap(__ ->
                        pgAsyncPool.executeMono(dsl -> dsl.query("""
                        CREATE UNIQUE INDEX IF NOT EXISTS WITHDRAW_BY_MONTH_UNIQUE_IDX ON WITHDRAW_BY_MONTH(client_id, month, year);    
                    """))
                )
                .flatMap(__ ->
                        pgAsyncPool.executeMono(dsl -> dsl.query("""
                        ALTER TABLE WITHDRAW_BY_MONTH
                        DROP CONSTRAINT IF EXISTS WITHDRAW_BY_MONTH_UNIQUE;    
                    """))
                )
                .flatMap(__ ->
                        pgAsyncPool.executeMono(dsl -> dsl.query("""
                        ALTER TABLE WITHDRAW_BY_MONTH
                        ADD CONSTRAINT WITHDRAW_BY_MONTH_UNIQUE 
                        UNIQUE USING INDEX WITHDRAW_BY_MONTH_UNIQUE_IDX;    
                    """))
                );
    }
}
