package com.example.demo;

import fr.maif.eventsourcing.EventHandler;
import io.vavr.control.Option;

import java.math.BigDecimal;

import static com.example.demo.BankEvent.$AccountClosed;
import static com.example.demo.BankEvent.$AccountOpened;
import static com.example.demo.BankEvent.$MoneyDeposited;
import static com.example.demo.BankEvent.$MoneyWithdrawn;
import static io.vavr.API.Case;
import static io.vavr.API.Match;

public class BankEventHandler implements EventHandler<Account, BankEvent> {
    @Override
    public Option<Account> applyEvent(
            Option<Account> previousState,
            BankEvent event) {
        return Match(event).of(
                Case($AccountOpened(), BankEventHandler::handleAccountOpened
                ),
                Case($MoneyDeposited(), deposit ->
                        BankEventHandler.handleMoneyDeposited(previousState, deposit)
                ),
                Case($MoneyWithdrawn(), deposit ->
                        BankEventHandler.handleMoneyWithdrawn(previousState, deposit)
                ),
                Case($AccountClosed(), BankEventHandler::handleAccountClosed)
        );
    }

    private static Option<Account> handleAccountClosed(
            BankEvent.AccountClosed close) {
        return Option.none();
    }

    private static Option<Account> handleMoneyWithdrawn(
            Option<Account> previousState,
            BankEvent.MoneyWithdrawn withdraw) {
        return previousState.map(account -> {
            account.balance = account.balance.subtract(withdraw.amount);
            return account;
        });
    }

    private static Option<Account> handleAccountOpened(BankEvent.AccountOpened event) {
        Account account = new Account();
        account.id = event.accountId;
        account.balance = BigDecimal.ZERO;

        return Option.some(account);
    }

    private static Option<Account> handleMoneyDeposited(
            Option<Account> previousState,
            BankEvent.MoneyDeposited event) {
        return previousState.map(state -> {
            state.balance = state.balance.add(event.amount);
            return state;
        });
    }
}
