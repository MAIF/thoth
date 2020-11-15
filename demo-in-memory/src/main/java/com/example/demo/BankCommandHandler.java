package com.example.demo;

import fr.maif.eventsourcing.CommandHandler;
import fr.maif.eventsourcing.Events;
import io.vavr.API;
import io.vavr.Tuple0;
import io.vavr.collection.List;
import io.vavr.concurrent.Future;
import io.vavr.control.Either;
import io.vavr.control.Option;

import java.math.BigDecimal;

import static io.vavr.API.*;

public class BankCommandHandler implements CommandHandler<String, Account, BankCommand, BankEvent, Tuple0, Tuple0> {
    @Override
    public Future<Either<String, Events<BankEvent, Tuple0>>> handleCommand(
            Tuple0 transactionContext,
            Option<Account> previousState,
            BankCommand command) {
        return Future.of(() -> Match(command).of(
            Case(BankCommand.WithdrawV1.pattern(), withdraw -> this.handleWithdraw(previousState, withdraw)),
            Case(BankCommand.DepositV1.pattern(), deposit -> this.handleDeposit(previousState, deposit)),
            Case(BankCommand.OpenAccountV1.pattern(), this::handleOpening),
                Case(BankCommand.CloseAccountV1.pattern(), close -> this.handleClosing(previousState, close))
        ));
    }

    private Either<String, Events<BankEvent, Tuple0>> handleOpening(
            BankCommand.OpenAccount opening) {
        if(opening.initialBalance.compareTo(BigDecimal.ZERO) < 0) {
            return Left("Initial balance can't be negative");
        }

        String newId = opening.id.get();
        List<BankEvent> events = List(new BankEvent.AccountOpened(newId));
        if(opening.initialBalance.compareTo(BigDecimal.ZERO) > 0) {
            events = events.append(new BankEvent.MoneyDeposited(newId, opening.initialBalance));
        }

        return Right(Events.events(events));
    }

    private Either<String, Events<BankEvent, Tuple0>> handleClosing(
            Option<Account> previousState,
            BankCommand.CloseAccount close) {
        return previousState.toEither("No account opened for this id : " + close.id)
                .map(state ->  Events.events(new BankEvent.AccountClosed(close.id)));
    }

    private Either<String, Events<BankEvent, Tuple0>> handleDeposit(
            Option<Account> previousState,
            BankCommand.Deposit deposit) {
        return previousState.toEither("Account does not exist")
                .map(account -> Events.events(new BankEvent.MoneyDeposited(deposit.account, deposit.amount)));
    }

    private Either<String, Events<BankEvent, Tuple0>> handleWithdraw(
            Option<Account> previousState,
            BankCommand.Withdraw withdraw) {
        return previousState.toEither("Account does not exist")
            .flatMap(previous -> {
                BigDecimal newBalance = previous.balance.subtract(withdraw.amount);
                if(newBalance.compareTo(BigDecimal.ZERO) < 0) {
                    return Left("Insufficient balance");
                }
                return Right(Events.events(new BankEvent.MoneyWithdrawn(withdraw.account, withdraw.amount)));
            });
    }
}