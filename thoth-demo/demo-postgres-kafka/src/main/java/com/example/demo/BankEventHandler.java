package com.example.demo;

import com.example.demo.BankEvent.AccountClosed;
import com.example.demo.BankEvent.AccountOpened;
import com.example.demo.BankEvent.MoneyDeposited;
import com.example.demo.BankEvent.MoneyWithdrawn;
import fr.maif.eventsourcing.vanilla.EventHandler;

import java.math.BigDecimal;
import java.util.Optional;

public class BankEventHandler implements EventHandler<Account, BankEvent> {
    @Override
    public Optional<Account> applyEvent(
            Optional<Account> previousState,
            BankEvent event) {
        return switch (event) {
            case AccountOpened accountOpened -> BankEventHandler.handleAccountOpened(accountOpened);
            case MoneyDeposited deposit -> BankEventHandler.handleMoneyDeposited(previousState, deposit);
            case MoneyWithdrawn deposit -> BankEventHandler.handleMoneyWithdrawn(previousState, deposit);
            case AccountClosed accountClosed -> BankEventHandler.handleAccountClosed(accountClosed);
        };
    }

    private static Optional<Account> handleAccountClosed(AccountClosed close) {
        return Optional.empty();
    }

    private static Optional<Account> handleMoneyWithdrawn(Optional<Account> previousState, MoneyWithdrawn withdraw) {
        return previousState.map(account -> {
            account.balance = account.balance.subtract(withdraw.amount());
            return account;
        });
    }

    private static Optional<Account> handleAccountOpened(AccountOpened event) {
        Account account = new Account();
        account.id = event.accountId();
        account.balance = BigDecimal.ZERO;

        return Optional.of(account);
    }

    private static Optional<Account> handleMoneyDeposited(
            Optional<Account> previousState,
            MoneyDeposited event) {
        return previousState.map(state -> {
            state.balance = state.balance.add(event.amount());
            return state;
        });
    }
}
