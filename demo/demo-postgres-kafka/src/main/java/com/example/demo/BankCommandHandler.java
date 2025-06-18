package com.example.demo;

import fr.maif.eventsourcing.vanilla.Events;
import fr.maif.eventsourcing.Result;
import fr.maif.eventsourcing.vanilla.blocking.CommandHandler;

import java.math.BigDecimal;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.example.demo.BankCommand.*;

public class BankCommandHandler implements CommandHandler<String, Account, BankCommand, BankEvent, List<String>, Connection> {
    
    @Override
    public Result<String, Events<BankEvent, List<String>>> handleCommand(
            Connection transactionContext,
            Optional<Account> previousState,
            BankCommand command) {
        return switch (command) {
            case Withdraw withdraw -> this.handleWithdraw(previousState, withdraw);
            case Deposit deposit -> this.handleDeposit(previousState, deposit);
            case OpenAccount openAccount -> this.handleOpening(openAccount);
            case CloseAccount close -> this.handleClosing(previousState, close);
        };
    }

    private Result<String, Events<BankEvent, List<String>>> handleOpening(
            BankCommand.OpenAccount opening) {
        if(opening.initialBalance().compareTo(BigDecimal.ZERO) < 0) {
            return Result.error("Initial balance can't be negative");
        }

        String newId = opening.id().get();
        List<BankEvent> events = new ArrayList<>();
        events.add(new BankEvent.AccountOpened(newId));
        if(opening.initialBalance().compareTo(BigDecimal.ZERO) > 0) {
            events.add(new BankEvent.MoneyDeposited(newId, opening.initialBalance()));
        }

        return events(List.<String>of(), events);
    }

    private Result<String, Events<BankEvent, List<String>>> handleClosing(
            Optional<Account> previousState,
            BankCommand.CloseAccount close) {
        return Result.fromOptional(previousState, () -> "No account opened for this id : " + close.id())
                .map(state ->  Events.events(List.of(), new BankEvent.AccountClosed(close.id())));
    }

    private Result<String, Events<BankEvent, List<String>>> handleDeposit(
            Optional<Account> previousState,
            BankCommand.Deposit deposit) {
        return Result.fromOptional(previousState, () -> "Account does not exist")
                .map(account -> Events.events(List.of(), new BankEvent.MoneyDeposited(deposit.account(), deposit.amount())));
    }

    private Result<String, Events<BankEvent, List<String>>> handleWithdraw(
            Optional<Account> previousState,
            BankCommand.Withdraw withdraw) {
        return Result.fromOptional(previousState, () -> "Account does not exist")
                .flatMap(previous -> {
                    BigDecimal newBalance = previous.balance.subtract(withdraw.amount());
                    List<String> messages = new ArrayList<>();
                    if(newBalance.compareTo(BigDecimal.ZERO) < 0) {
                        messages.add("Overdrawn account");
                    }
                    return Result.success(Events.events(messages, new BankEvent.MoneyWithdrawn(withdraw.account(), withdraw.amount())));
                });
    }
}