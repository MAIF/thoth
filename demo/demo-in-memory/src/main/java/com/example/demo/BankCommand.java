package com.example.demo;

import fr.maif.eventsourcing.SimpleCommand;
import fr.maif.eventsourcing.Type;
import io.vavr.API;
import io.vavr.Lazy;

import java.math.BigDecimal;

public sealed interface BankCommand extends SimpleCommand {

    record Withdraw(String account, BigDecimal amount) implements BankCommand {
        @Override
        public Lazy<String> entityId() {
            return Lazy.of(() -> account);
        }
    }

    record OpenAccount(Lazy<String> id, BigDecimal initialBalance) implements BankCommand {

        @Override
        public Lazy<String> entityId() {
            return id;
        }

        @Override
        public Boolean hasId() {
            return false;
        }
    }

    record Deposit(String account, BigDecimal amount) implements BankCommand {
        @Override
        public Lazy<String> entityId() {
            return Lazy.of(() -> account);
        }
    }

    record CloseAccount(String id) implements BankCommand {
        @Override
        public Lazy<String> entityId() {
            return Lazy.of(() -> id);
        }
    }
}
