package com.example.demo;

import fr.maif.eventsourcing.Event;
import fr.maif.eventsourcing.Type;
import io.vavr.API;
import io.vavr.API.Match.Pattern0;

import java.math.BigDecimal;

public abstract class BankEvent implements Event {
    public static Type<MoneyWithdrawn> MoneyWithdrawnV1 = Type.create(MoneyWithdrawn.class, 1L);
    public static Type<AccountOpened> AccountOpenedV1 = Type.create(AccountOpened.class, 1L);
    public static Type<MoneyDeposited> MoneyDepositedV1 = Type.create(MoneyDeposited.class, 1L);
    public static Type<AccountClosed> AccountClosedV1 = Type.create(AccountClosed.class, 1L);

    static Pattern0<AccountOpened> $AccountOpened() {
        return Pattern0.of(AccountOpened.class);
    }
    static Pattern0<MoneyWithdrawn> $MoneyWithdrawn() {
        return Pattern0.of(MoneyWithdrawn.class);
    }
    static Pattern0<MoneyDeposited> $MoneyDeposited() {
        return Pattern0.of(MoneyDeposited.class);
    }
    static Pattern0<AccountClosed> $AccountClosed() {
        return Pattern0.of(AccountClosed.class);
    }

    public final String accountId;

    public BankEvent(String accountId) {
        this.accountId = accountId;
    }

    @Override
    public String entityId() {
        return accountId;
    }

    public static class MoneyWithdrawn extends BankEvent {
        public final BigDecimal amount;
        public MoneyWithdrawn(String account, BigDecimal amount) {
            super(account);
            this.amount = amount;
        }

        @Override
        public Type<MoneyWithdrawn> type() {
            return MoneyWithdrawnV1;
        }
    }

    public static class AccountOpened extends BankEvent {
        public AccountOpened(String id) {
            super(id);
        }

        @Override
        public Type<AccountOpened> type() {
            return AccountOpenedV1;
        }
    }

    public static class MoneyDeposited extends BankEvent {
        public final BigDecimal amount;

        public MoneyDeposited(String id, BigDecimal amount) {
            super(id);
            this.amount = amount;
        }

        @Override
        public Type<MoneyDeposited> type() {
            return MoneyDepositedV1;
        }
    }

    public static class AccountClosed extends BankEvent {

        public AccountClosed(String id) {
            super(id);
        }

        @Override
        public Type<AccountClosed> type() {
            return AccountClosedV1;
        }
    }
}
