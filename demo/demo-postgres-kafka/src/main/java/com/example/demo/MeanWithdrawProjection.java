package com.example.demo;

import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.eventsourcing.Projection;
import io.vavr.Tuple;
import io.vavr.Tuple0;
import io.vavr.collection.List;
import io.vavr.concurrent.Future;

import java.math.BigDecimal;
import java.sql.Connection;

public class MeanWithdrawProjection implements Projection<Connection, BankEvent, Tuple0, Tuple0> {
    private BigDecimal withDrawTotal = BigDecimal.ZERO;
    private long withdrawCount = 0L;

    @Override
    public Future<Tuple0> storeProjection(Connection connection, List<EventEnvelope<BankEvent, Tuple0, Tuple0>> envelopes) {
        return Future.of(() -> {
            envelopes.forEach(envelope -> {
                BankEvent bankEvent = envelope.event;
                if(envelope.event instanceof BankEvent.MoneyWithdrawn) {
                    withDrawTotal = withDrawTotal.add(((BankEvent.MoneyWithdrawn)bankEvent).amount);
                    withdrawCount ++;
                }
            });

            return Tuple.empty();
        });
    }

    public BigDecimal meanWithdraw() {
        return withDrawTotal.divide(BigDecimal.valueOf(withdrawCount));
    }
}
