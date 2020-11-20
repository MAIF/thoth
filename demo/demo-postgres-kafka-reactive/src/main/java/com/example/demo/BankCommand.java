package com.example.demo;

import fr.maif.eventsourcing.SimpleCommand;
import io.vavr.API.Match.Pattern0;
import io.vavr.Lazy;

import java.math.BigDecimal;

public interface BankCommand extends SimpleCommand {
    
    static Pattern0<Withdraw> $Withdraw() {
        return Pattern0.of(Withdraw.class);
    }

    static Pattern0<OpenAccount> $OpenAccount() {
        return Pattern0.of(OpenAccount.class);
    }

    static Pattern0<Deposit> $Deposit() {
        return Pattern0.of(Deposit.class);
    }

    static Pattern0<CloseAccount> $CloseAccount() {
        return Pattern0.of(CloseAccount.class);
    }

    class Withdraw implements BankCommand {
        public String account;
        public BigDecimal amount;

        public Withdraw(String account, BigDecimal amount) {
            this.account = account;
            this.amount = amount;
        }

        @Override
        public Lazy<String> entityId() {
            return Lazy.of(() -> account);
        }
    }

    class OpenAccount implements BankCommand {
        public Lazy<String> id;
        public BigDecimal initialBalance;

        public OpenAccount(Lazy<String> id, BigDecimal initialBalance) {
            this.initialBalance = initialBalance;
            this.id = id;
        }

        @Override
        public Lazy<String> entityId() {
            return id;
        }

        @Override
        public Boolean hasId() {
            return false;
        }
    }

    class Deposit implements BankCommand {
        public String account;
        public BigDecimal amount;

        public Deposit(String account, BigDecimal amount) {
            this.account = account;
            this.amount = amount;
        }

        @Override
        public Lazy<String> entityId() {
            return Lazy.of(() -> account);
        }
    }

    class CloseAccount implements BankCommand {
        public String id;

        public CloseAccount(String id) {
            this.id = id;
        }

        @Override
        public Lazy<String> entityId() {
            return Lazy.of(() -> id);
        }
    }
}
