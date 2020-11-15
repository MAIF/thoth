package com.example.demo;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import fr.maif.eventsourcing.Event;
import fr.maif.eventsourcing.Type;

import java.math.BigDecimal;

public abstract class BankEvent implements Event {
    public static Type<MoneyWithdrawn> MoneyWithdrawnV1 = Type.create(MoneyWithdrawn.class, 1L);
    public static Type<AccountOpened> AccountOpenedV1 = Type.create(AccountOpened.class, 1L);
    public static Type<MoneyDeposited> MoneyDepositedV1 = Type.create(MoneyDeposited.class, 1L);
    public static Type<AccountClosed> AccountClosedV1 = Type.create(AccountClosed.class, 1L);

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
        @JsonCreator
        public MoneyWithdrawn(@JsonProperty("accountId")String account, @JsonProperty("amount")BigDecimal amount) {
            super(account);
            this.amount = amount;
        }

        @Override
        public Type<?> type() {
            return MoneyWithdrawnV1;
        }
    }

    public static class AccountOpened extends BankEvent {
        @JsonCreator
        public AccountOpened(@JsonProperty("accountId")String id) {
            super(id);
        }

        @Override
        public Type<?> type() {
            return AccountOpenedV1;
        }
    }

    public static class MoneyDeposited extends BankEvent {
        public final BigDecimal amount;
        @JsonCreator
        public MoneyDeposited(@JsonProperty("accountId")String id, @JsonProperty("amount")BigDecimal amount) {
            super(id);
            this.amount = amount;
        }

        @Override
        public Type<?> type() {
            return MoneyDepositedV1;
        }
    }

    public static class AccountClosed extends BankEvent {
        @JsonCreator
        public AccountClosed(@JsonProperty("accountId")String id) {
            super(id);
        }

        @Override
        public Type<?> type() {
            return AccountClosedV1;
        }
    }
}
