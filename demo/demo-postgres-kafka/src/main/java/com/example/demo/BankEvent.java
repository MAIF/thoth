package com.example.demo;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import fr.maif.eventsourcing.Event;
import fr.maif.eventsourcing.Type;
import io.vavr.API;
import io.vavr.API.Match.Pattern0;

import java.math.BigDecimal;

public sealed interface BankEvent extends Event {
    Type<MoneyWithdrawn> MoneyWithdrawnV1 = Type.create(MoneyWithdrawn.class, 1L);
    Type<AccountOpened> AccountOpenedV1 = Type.create(AccountOpened.class, 1L);
    Type<MoneyDeposited> MoneyDepositedV1 = Type.create(MoneyDeposited.class, 1L);
    Type<AccountClosed> AccountClosedV1 = Type.create(AccountClosed.class, 1L);

    String accountId();

    default String entityId() {
        return accountId();
    }

    record MoneyWithdrawn(String accountId, BigDecimal amount) implements BankEvent {
        @Override
        public Type<MoneyWithdrawn> type() {
            return MoneyWithdrawnV1;
        }
    }

    record AccountOpened(String accountId) implements BankEvent {
        @Override
        public Type<AccountOpened> type() {
            return AccountOpenedV1;
        }
    }

    record MoneyDeposited(String accountId, BigDecimal amount) implements BankEvent {
        @Override
        public Type<MoneyDeposited> type() {
            return MoneyDepositedV1;
        }
    }

    record AccountClosed(String accountId) implements BankEvent {
        @Override
        public Type<AccountClosed> type() {
            return AccountClosedV1;
        }
    }
}