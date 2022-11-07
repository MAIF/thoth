package com.example.demo;

import fr.maif.eventsourcing.EventHandler;
import io.vavr.control.Option;

import java.math.BigDecimal;

public class BankEventHandler implements EventHandler<Account, BankEvent> {
    @Override
    public Option<Account> applyEvent(
            Option<Account> previousState,
            BankEvent event) {
        return switch (event) {
            case BankEvent.AccountOpened accountOpened -> BankEventHandler.handleAccountOpened(accountOpened);
            case BankEvent.MoneyDeposited deposit -> BankEventHandler.handleMoneyDeposited(previousState, deposit);
            case BankEvent.MoneyWithdrawn withdraw -> BankEventHandler.handleMoneyWithdrawn(previousState, withdraw);
            case BankEvent.AccountClosed accountClosed -> BankEventHandler.handleAccountClosed(accountClosed);
        };
    }

    private static Option<Account> handleAccountClosed(
            BankEvent.AccountClosed close) {
        return Option.none();
    }

    private static Option<Account> handleMoneyWithdrawn(
            Option<Account> previousState,
            BankEvent.MoneyWithdrawn withdraw) {
        return previousState.map(account -> {
            account.balance = account.balance.subtract(withdraw.amount());
            return account;
        });
    }

    private static Option<Account> handleAccountOpened(BankEvent.AccountOpened event) {
        Account account = new Account();
        account.id = event.accountId();
        account.balance = BigDecimal.ZERO;

        return Option.some(account);
    }

    private static Option<Account> handleMoneyDeposited(
            Option<Account> previousState,
            BankEvent.MoneyDeposited event) {
        return previousState.map(state -> {
            state.balance = state.balance.add(event.amount());
            return state;
        });
    }
}
