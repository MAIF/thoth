package com.example.demo;

import fr.maif.eventsourcing.vanilla.EventHandler;

import java.math.BigDecimal;
import java.util.Optional;

import static com.example.demo.BankEvent.AccountClosed;
import static com.example.demo.BankEvent.AccountOpened;
import static com.example.demo.BankEvent.MoneyDeposited;
import static com.example.demo.BankEvent.MoneyWithdrawn;

public class BankEventHandler implements EventHandler<Account, BankEvent> {
    @Override
    public Optional<Account> applyEvent(
            Optional<Account> previousState,
            BankEvent event) {
        return switch (event) {
            case AccountOpened accountOpened -> BankEventHandler.handleAccountOpened(accountOpened);
            case MoneyDeposited deposit -> BankEventHandler.handleMoneyDeposited(previousState, deposit);
            case MoneyWithdrawn withdraw -> BankEventHandler.handleMoneyWithdrawn(previousState, withdraw);
            case AccountClosed accountClosed -> BankEventHandler.handleAccountClosed(accountClosed);
        };
    }

    private static Optional<Account> handleAccountClosed(BankEvent.AccountClosed close) {
        return Optional.empty();
    }

    private static Optional<Account> handleMoneyWithdrawn(
            Optional<Account> previousState,
            BankEvent.MoneyWithdrawn withdraw) {
        return previousState.map(account -> {
            account.balance = account.balance.subtract(withdraw.amount());
            return account;
        });
    }

    private static Optional<Account> handleAccountOpened(BankEvent.AccountOpened event) {
        Account account = new Account();
        account.id = event.accountId();
        account.balance = BigDecimal.ZERO;

        return Optional.of(account);
    }

    private static Optional<Account> handleMoneyDeposited(
            Optional<Account> previousState,
            BankEvent.MoneyDeposited event) {
        return previousState.map(state -> {
            state.balance = state.balance.add(event.amount());
            return state;
        });
    }
}
