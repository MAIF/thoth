package com.example.demo;

import fr.maif.eventsourcing.Lazy;
import fr.maif.eventsourcing.vanilla.SimpleCommand;

import java.math.BigDecimal;

public sealed interface BankCommand extends SimpleCommand {

    record Withdraw(String account, BigDecimal amount) implements BankCommand {
        @Override
        public Lazy<String> getEntityId() {
            return Lazy.of(() -> account);
        }
    }

    record OpenAccount(Lazy<String> id, BigDecimal initialBalance) implements BankCommand {
        @Override
        public Lazy<String> getEntityId() {
            return id;
        }

        @Override
        public Boolean hasId() {
            return false;
        }
    }

    record Deposit(String account, BigDecimal amount) implements BankCommand {
        @Override
        public Lazy<String> getEntityId() {
            return Lazy.of(() -> account);
        }
    }

    record CloseAccount(String id) implements BankCommand {
        @Override
        public Lazy<String> getEntityId() {
            return Lazy.of(() -> id);
        }
    }
}
