package com.example.demo;

import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.eventsourcing.Unit;
import fr.maif.eventsourcing.vanilla.Projection;

import java.math.BigDecimal;
import java.sql.Connection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class MeanWithdrawProjection implements Projection<Connection, BankEvent, Unit, Unit> {
    private BigDecimal withDrawTotal = BigDecimal.ZERO;
    private long withdrawCount = 0L;

    @Override
    public CompletionStage<Void> storeProjection(Connection connection, List<EventEnvelope<BankEvent, Unit, Unit>> envelopes) {
        return CompletableFuture.runAsync(() -> {
            envelopes.forEach(envelope -> {
                BankEvent bankEvent = envelope.event;
                if(envelope.event instanceof BankEvent.MoneyWithdrawn) {
                    withDrawTotal = withDrawTotal.add(((BankEvent.MoneyWithdrawn)bankEvent).amount());
                    withdrawCount ++;
                }
            });
        });
    }

    public BigDecimal meanWithdraw() {
        return withDrawTotal.divide(BigDecimal.valueOf(withdrawCount));
    }
}
