package com.example.demo;

import com.example.demo.BankCommand.CloseAccount;
import com.example.demo.BankCommand.Deposit;
import com.example.demo.BankCommand.OpenAccount;
import com.example.demo.BankCommand.Withdraw;
import fr.maif.eventsourcing.Events;
import fr.maif.eventsourcing.ReactorCommandHandler;
import fr.maif.jooq.reactor.PgAsyncTransaction;
import io.vavr.Tuple0;
import io.vavr.collection.List;
import io.vavr.control.Either;
import io.vavr.control.Option;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;

import static io.vavr.API.Left;
import static io.vavr.API.List;
import static io.vavr.API.Right;

public class BankCommandHandler implements ReactorCommandHandler<String, Account, BankCommand, BankEvent, Tuple0, PgAsyncTransaction> {
    @Override
    public Mono<Either<String, Events<BankEvent, Tuple0>>> handleCommand(
            PgAsyncTransaction transactionContext,
            Option<Account> previousState,
            BankCommand command) {
        return Mono.fromCallable(() -> switch (command) {
            case Withdraw withdraw -> this.handleWithdraw(previousState, withdraw);
            case Deposit deposit -> this.handleDeposit(previousState, deposit);
            case OpenAccount openAccount -> this.handleOpening(openAccount);
            case CloseAccount close -> this.handleClosing(previousState, close);
        });
    }

    private Either<String, Events<BankEvent, Tuple0>> handleOpening(
            OpenAccount opening) {
        if(opening.initialBalance().compareTo(BigDecimal.ZERO) < 0) {
            return Left("Initial balance can't be negative");
        }

        String newId = opening.id().get();
        List<BankEvent> events = List(new BankEvent.AccountOpened(newId));
        if(opening.initialBalance().compareTo(BigDecimal.ZERO) > 0) {
            events = events.append(new BankEvent.MoneyDeposited(newId, opening.initialBalance()));
        }

        return Right(Events.events(events));
    }

    private Either<String, Events<BankEvent, Tuple0>> handleClosing(
            Option<Account> previousState,
            CloseAccount close) {
        return previousState.toEither("No account opened for this id : " + close.id())
                .map(state ->  Events.events(new BankEvent.AccountClosed(close.id())));
    }

    private Either<String, Events<BankEvent, Tuple0>> handleDeposit(
            Option<Account> previousState,
            Deposit deposit) {
        return previousState.toEither("Account does not exist")
                .map(account -> Events.events(new BankEvent.MoneyDeposited(deposit.account(), deposit.amount())));
    }

    private Either<String, Events<BankEvent, Tuple0>> handleWithdraw(
            Option<Account> previousState,
            Withdraw withdraw) {
        return previousState.toEither("Account does not exist")
                .flatMap(previous -> {
                    BigDecimal newBalance = previous.balance.subtract(withdraw.amount());
                    if(newBalance.compareTo(BigDecimal.ZERO) < 0) {
                        return Left("Insufficient balance");
                    }
                    return Right(Events.events(new BankEvent.MoneyWithdrawn(withdraw.account(), withdraw.amount())));
                });
    }
}