package com.example.demo;

import fr.maif.concurrent.CompletionStages;
import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.eventsourcing.Projection;
import io.vavr.Tuple;
import io.vavr.Tuple0;
import io.vavr.collection.List;
import io.vavr.concurrent.Future;

import java.math.BigDecimal;
import java.sql.Connection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class MeanWithdrawProjection implements Projection<Connection, BankEvent, Tuple0, Tuple0> {
    private BigDecimal withDrawTotal = BigDecimal.ZERO;
    private long withdrawCount = 0L;

    @Override
    public CompletionStage<Void> storeProjection(Connection connection, List<EventEnvelope<BankEvent, Tuple0, Tuple0>> envelopes) {
        return CompletableFuture.runAsync(() -> {
            envelopes.forEach(envelope -> {
                BankEvent bankEvent = envelope.event;
                if(envelope.event instanceof BankEvent.MoneyWithdrawn) {
                    withDrawTotal = withDrawTotal.add(((BankEvent.MoneyWithdrawn)bankEvent).amount);
                    withdrawCount ++;
                }
            });
        });
    }

    public BigDecimal meanWithdraw() {
        return withDrawTotal.divide(BigDecimal.valueOf(withdrawCount));
    }
}
