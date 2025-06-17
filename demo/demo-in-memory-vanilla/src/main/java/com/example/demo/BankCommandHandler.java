package com.example.demo;

import com.example.demo.BankCommand.CloseAccount;
import com.example.demo.BankCommand.Deposit;
import com.example.demo.BankCommand.OpenAccount;
import com.example.demo.BankCommand.Withdraw;
import fr.maif.eventsourcing.Events;
import fr.maif.eventsourcing.Result;
import fr.maif.eventsourcing.Unit;
import fr.maif.eventsourcing.vanilla.blocking.CommandHandler;
import fr.maif.reactor.eventsourcing.vanilla.InMemoryEventStore.Transaction;


import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;


public class BankCommandHandler implements CommandHandler<String, Account, BankCommand, BankEvent, Unit, Transaction<BankEvent, Unit, Unit>> {
    @Override
    public Result<String, Events<BankEvent, Unit>> handleCommand(
            Transaction<BankEvent, Unit, Unit> transactionContext,
            Optional<Account> previousState,
            BankCommand command) {
        return switch (command) {
            case Withdraw withdraw -> this.handleWithdraw(previousState, withdraw);
            case Deposit deposit -> this.handleDeposit(previousState, deposit);
            case OpenAccount openAccount -> this.handleOpening(openAccount);
            case CloseAccount close -> this.handleClosing(previousState, close);
        };
    }

    private Result<String, Events<BankEvent, Unit>> handleOpening(
            OpenAccount opening) {
        if (opening.initialBalance().compareTo(BigDecimal.ZERO) < 0) {
            return Result.error("Initial balance can't be negative");
        }

        String newId = opening.id().get();
        List<BankEvent> events = List.of(new BankEvent.AccountOpened(newId));
        if (opening.initialBalance().compareTo(BigDecimal.ZERO) > 0) {
            events = Stream.concat(events.stream(), Stream.of(new BankEvent.MoneyDeposited(newId, opening.initialBalance()))).toList();
        }

        return events(events);
    }

    private Result<String, Events<BankEvent, Unit>> handleClosing(
            Optional<Account> previousState,
            CloseAccount close) {
        return previousState
                .map(state -> events(new BankEvent.AccountClosed(close.id())))
                .orElseGet(() -> Result.error("No account opened for this id : " + close.id()));
    }

    private Result<String, Events<BankEvent, Unit>> handleDeposit(
            Optional<Account> previousState,
            Deposit deposit) {
        return previousState
                .map(state -> events(new BankEvent.MoneyDeposited(deposit.account(), deposit.amount())))
                .orElseGet(() -> Result.error("Account does not exist"));
    }

    private Result<String, Events<BankEvent, Unit>> handleWithdraw(
            Optional<Account> previousState,
            Withdraw withdraw) {
        return Result.fromOptional(previousState, () -> "Account does not exist")
                .flatMap(previous -> {
                    BigDecimal newBalance = previous.balance.subtract(withdraw.amount());
                    if (newBalance.compareTo(BigDecimal.ZERO) < 0) {
                        return Result.error("Insufficient balance");
                    }
                    return events(new BankEvent.MoneyWithdrawn(withdraw.account(), withdraw.amount()));
                });
    }
}