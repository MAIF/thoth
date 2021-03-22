package com.example.demo;

import akka.actor.ActorSystem;
import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.impl.TimeBasedGenerator;
import fr.maif.eventsourcing.EventProcessor;
import fr.maif.eventsourcing.ProcessingSuccess;
import fr.maif.eventsourcing.TransactionManager;
import fr.maif.eventsourcing.impl.InMemoryEventStore;
import io.vavr.Lazy;
import io.vavr.Tuple;
import io.vavr.Tuple0;
import io.vavr.collection.List;
import io.vavr.concurrent.Future;
import io.vavr.control.Either;
import io.vavr.control.Option;

import java.math.BigDecimal;
import java.util.function.Function;

public class Bank {
    private final EventProcessor<String, Account, BankCommand, BankEvent, Tuple0, Tuple0, Tuple0, Tuple0> eventProcessor;
    private static final TimeBasedGenerator UUIDgenerator = Generators.timeBasedGenerator();


    public Bank(ActorSystem actorSystem,
                BankCommandHandler commandHandler,
                BankEventHandler eventHandler
                ) {
        InMemoryEventStore<BankEvent, Tuple0, Tuple0> eventStore = InMemoryEventStore.create(actorSystem);
        this.eventProcessor = new EventProcessor<>(
                actorSystem,
                eventStore,
                noOpTransactionManager(),
                commandHandler,
                eventHandler,
                List.empty()
        );
    }

    private TransactionManager<Tuple0> noOpTransactionManager() {
        return new TransactionManager<>() {
            @Override
            public <T> Future<T> withTransaction(Function<Tuple0, Future<T>> function) {
                return function.apply(Tuple.empty());
            }
        };
    }

    public Future<Either<String, ProcessingSuccess<Account, BankEvent, Tuple0, Tuple0, Tuple0>>> createAccount(
            BigDecimal amount) {
        Lazy<String> lazyId = Lazy.of(() -> UUIDgenerator.generate().toString());
        return eventProcessor.processCommand(new BankCommand.OpenAccount(lazyId, amount));
    }

    public Future<Either<String, ProcessingSuccess<Account, BankEvent, Tuple0, Tuple0, Tuple0>>> withdraw(
            String account, BigDecimal amount) {
        return eventProcessor.processCommand(new BankCommand.Withdraw(account, amount));
    }

    public Future<Either<String, ProcessingSuccess<Account, BankEvent, Tuple0, Tuple0, Tuple0>>> deposit(
            String account, BigDecimal amount) {
        return eventProcessor.processCommand(new BankCommand.Deposit(account, amount));
    }

    public Future<Either<String, ProcessingSuccess<Account, BankEvent, Tuple0, Tuple0, Tuple0>>> close(
            String account) {
        return eventProcessor.processCommand(new BankCommand.CloseAccount(account));
    }

    public Future<Option<Account>> findAccountById(String id) {
        return eventProcessor.getAggregate(id);
    }
}
