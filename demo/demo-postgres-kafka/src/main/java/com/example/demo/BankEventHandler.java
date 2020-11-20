package com.example.demo;

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
        return Match(event).of(
                Case(BankEvent.AccountOpenedV1.pattern(), BankEventHandler::handleAccountOpened
                ),
                Case(BankEvent.MoneyDepositedV1.pattern(),
                        deposit -> BankEventHandler.handleMoneyDeposited(previousState, deposit)
                ),
                Case(BankEvent.MoneyWithdrawnV1.pattern(),
                        withdraw -> BankEventHandler.handleMoneyWithdrawn(previousState, withdraw)
                ),
                Case(BankEvent.AccountClosedV1.pattern(), BankEventHandler::handleAccountClosed
                )
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
