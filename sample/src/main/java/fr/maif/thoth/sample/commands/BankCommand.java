package fr.maif.thoth.sample.commands;

import java.math.BigDecimal;

import fr.maif.eventsourcing.SimpleCommand;
import fr.maif.eventsourcing.Type;
import io.vavr.API.Match.Pattern0;
import io.vavr.Lazy;

public sealed interface BankCommand extends SimpleCommand {

    final class Withdraw implements BankCommand {
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

    final class OpenAccount implements BankCommand {
        public String id;
        public BigDecimal initialBalance;

        public OpenAccount(String id, BigDecimal initialBalance) {
            this.initialBalance = initialBalance;
            this.id = id;
        }

        @Override
        public Lazy<String> entityId() {
            return Lazy.of(() -> id);
        }
    }

    final class Deposit implements BankCommand {
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

    final class CloseAccount implements BankCommand {
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
