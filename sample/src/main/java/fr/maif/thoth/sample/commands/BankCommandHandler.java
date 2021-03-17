package fr.maif.thoth.sample.commands;

import static io.vavr.API.*;
import static io.vavr.API.Left;
import static io.vavr.API.Right;

import java.math.BigDecimal;
import java.sql.Connection;

import fr.maif.eventsourcing.CommandHandler;
import fr.maif.eventsourcing.Events;
import fr.maif.thoth.sample.events.BankEvent;
import fr.maif.thoth.sample.state.Account;
import io.vavr.Tuple0;
import io.vavr.collection.List;
import io.vavr.concurrent.Future;
import io.vavr.control.Either;
import io.vavr.control.Option;

public class BankCommandHandler implements CommandHandler<String, Account, BankCommand, BankEvent, Tuple0, Connection> {
    @Override
    public Future<Either<String, Events<BankEvent, Tuple0>>> handleCommand(
            Connection transactionContext,
            Option<Account> previousState,
            BankCommand command) {
        return Future.of(() -> {
            if(command instanceof BankCommand.Withdraw withdraw) {
                return this.handleWithdraw(previousState, withdraw);
            } else if(command instanceof BankCommand.Deposit deposit) {
                return this.handleDeposit(previousState, deposit);
            } else if(command instanceof BankCommand.CloseAccount close) {
                return this.handleClosing(previousState, close);
            } else if(command instanceof BankCommand.OpenAccount opening) {
                return this.handleOpening(previousState, opening);
            } else {
                return Either.left("Unknown command type for command " + command);
            }
        });
    }

    private Either<String, Events<BankEvent, Tuple0>> handleOpening(
            Option<Account> previousState,
            BankCommand.OpenAccount opening) {
        if (previousState.isDefined()) {
            return Left("An account already exists for the id: " + previousState.get().id);
        }
        if (opening.initialBalance.compareTo(BigDecimal.ZERO) < 0) {
            return Left("Initial balance can't be negative");
        }

        String newId = opening.id;
        List<BankEvent> events = List(new BankEvent.AccountOpened(newId));

        if(opening.initialBalance.compareTo(BigDecimal.ZERO) > 0) {
            events = events.append(new BankEvent.MoneyDeposited(newId, opening.initialBalance));
        }

        return Right(Events.events(
                Tuple0.instance(), // Messages (for instance warnings) to be returned
                events
        ));
    }

    private Either<String, Events<BankEvent, Tuple0>> handleClosing(
            Option<Account> previousState,
            BankCommand.CloseAccount close) {
        return previousState.toEither("Account does not exist")
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
                    return Either.left("Overdrawn account");
                }
                return events(new BankEvent.MoneyWithdrawn(withdraw.account, withdraw.amount));
            });
    }
}