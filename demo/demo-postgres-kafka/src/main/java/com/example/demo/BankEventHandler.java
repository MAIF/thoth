package com.example.demo;

import com.example.demo.BankEvent.AccountClosed;
import com.example.demo.BankEvent.AccountOpened;
import com.example.demo.BankEvent.MoneyDeposited;
import com.example.demo.BankEvent.MoneyWithdrawn;
import fr.maif.eventsourcing.EventHandler;
import io.vavr.control.Option;

import java.math.BigDecimal;

import static io.vavr.API.Case;
import static io.vavr.API.Match;

public class BankEventHandler implements EventHandler<Account, BankEvent> {
    @Override
    public Option<Account> applyEvent(
            Option<Account> previousState,
            BankEvent event) {
        return switch (event) {
            case AccountOpened accountOpened -> BankEventHandler.handleAccountOpened(accountOpened);
            case MoneyDeposited deposit -> BankEventHandler.handleMoneyDeposited(previousState, deposit);
            case MoneyWithdrawn deposit -> BankEventHandler.handleMoneyWithdrawn(previousState, deposit);
            case AccountClosed accountClosed -> BankEventHandler.handleAccountClosed(accountClosed);
        };
    }

    private static Option<Account> handleAccountClosed(
            AccountClosed close) {
        return Option.none();
    }

    private static Option<Account> handleMoneyWithdrawn(
            Option<Account> previousState,
            MoneyWithdrawn withdraw) {
        return previousState.map(account -> {
            account.balance = account.balance.subtract(withdraw.amount());
            return account;
        });
    }

    private static Option<Account> handleAccountOpened(AccountOpened event) {
        Account account = new Account();
        account.id = event.accountId();
        account.balance = BigDecimal.ZERO;

        return Option.some(account);
    }

    private static Option<Account> handleMoneyDeposited(
            Option<Account> previousState,
            MoneyDeposited event) {
        return previousState.map(state -> {
            state.balance = state.balance.add(event.amount());
            return state;
        });
    }
}
