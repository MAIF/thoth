package com.example.demo;

import fr.maif.concurrent.CompletionStages;
import fr.maif.eventsourcing.CommandHandler;
import fr.maif.eventsourcing.Events;
import io.vavr.collection.List;
import io.vavr.control.Either;
import io.vavr.control.Option;

import java.math.BigDecimal;
import java.sql.Connection;
import java.util.concurrent.CompletionStage;

import static com.example.demo.BankCommand.CloseAccount;
import static com.example.demo.BankCommand.Deposit;
import static com.example.demo.BankCommand.OpenAccount;
import static com.example.demo.BankCommand.Withdraw;
import static io.vavr.API.Left;
import static io.vavr.API.List;
import static io.vavr.API.Right;

public class BankCommandHandler implements CommandHandler<String, Account, BankCommand, BankEvent, List<String>, Connection> {
    @Override
    public CompletionStage<Either<String, Events<BankEvent, List<String>>>> handleCommand(
            Connection transactionContext,
            Option<Account> previousState,
            BankCommand command) {
        return CompletionStages.of(() -> switch (command) {
            case Withdraw withdraw -> this.handleWithdraw(previousState, withdraw);
            case Deposit deposit -> this.handleDeposit(previousState, deposit);
            case OpenAccount openAccount -> this.handleOpening(openAccount);
            case CloseAccount close -> this.handleClosing(previousState, close);
        });
    }

    private Either<String, Events<BankEvent, List<String>>> handleOpening(
            BankCommand.OpenAccount opening) {
        if(opening.initialBalance().compareTo(BigDecimal.ZERO) < 0) {
            return Left("Initial balance can't be negative");
        }

        String newId = opening.id().get();
        List<BankEvent> events = List(new BankEvent.AccountOpened(newId));
        if(opening.initialBalance().compareTo(BigDecimal.ZERO) > 0) {
            events = events.append(new BankEvent.MoneyDeposited(newId, opening.initialBalance()));
        }

        return Right(Events.events(List.empty(), events));
    }

    private Either<String, Events<BankEvent, List<String>>> handleClosing(
            Option<Account> previousState,
            BankCommand.CloseAccount close) {
        return previousState.toEither("No account opened for this id : " + close.id())
                .map(state ->  Events.events(List.empty(), new BankEvent.AccountClosed(close.id())));
    }

    private Either<String, Events<BankEvent, List<String>>> handleDeposit(
            Option<Account> previousState,
            BankCommand.Deposit deposit) {
        return previousState.toEither("Account does not exist")
                .map(account -> Events.events(List.empty(), new BankEvent.MoneyDeposited(deposit.account(), deposit.amount())));
    }

    private Either<String, Events<BankEvent, List<String>>> handleWithdraw(
            Option<Account> previousState,
            BankCommand.Withdraw withdraw) {
        return previousState.toEither("Account does not exist")
                .flatMap(previous -> {
                    BigDecimal newBalance = previous.balance.subtract(withdraw.amount());
                    List<String> messages = List();
                    if(newBalance.compareTo(BigDecimal.ZERO) < 0) {
                        messages = messages.push("Overdrawn account");
                    }
                    return Right(Events.events(messages, new BankEvent.MoneyWithdrawn(withdraw.account(), withdraw.amount())));
                });
    }
}