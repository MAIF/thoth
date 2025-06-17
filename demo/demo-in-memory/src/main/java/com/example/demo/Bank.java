package com.example.demo;

import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.impl.TimeBasedGenerator;
import fr.maif.eventsourcing.*;
import fr.maif.reactor.eventsourcing.DefaultAggregateStore;
import fr.maif.reactor.eventsourcing.InMemoryEventStore;
import fr.maif.eventsourcing.vanilla.EventProcessorVanilla;
import fr.maif.reactor.eventsourcing.InMemoryEventStore.Transaction;
import io.vavr.Lazy;
import io.vavr.Tuple;
import io.vavr.Tuple0;
import io.vavr.collection.List;
import io.vavr.control.Either;
import io.vavr.control.Option;

import java.math.BigDecimal;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

public class Bank {
    private final EventProcessor<String, Account, BankCommand, BankEvent, Transaction<BankEvent, Tuple0, Tuple0>, Tuple0, Tuple0, Tuple0> eventProcessor;
    private final fr.maif.eventsourcing.vanilla.EventProcessor<String, Account, BankCommand, BankEvent, Transaction<BankEvent, Tuple0, Tuple0>, Tuple0, Tuple0, Tuple0> eventProcessorVanilla;
    private static final TimeBasedGenerator UUIDgenerator = Generators.timeBasedGenerator();


    public Bank(
                BankCommandHandler commandHandler,
                BankEventHandler eventHandler) {
        EventStore<Transaction<BankEvent, Tuple0, Tuple0>, BankEvent, Tuple0, Tuple0> eventStore = InMemoryEventStore.create();
        TransactionManager<Transaction<BankEvent, Tuple0, Tuple0>> transactionManager = noOpTransactionManager();
        ExecutorService executor = Executors.newCachedThreadPool();
        this.eventProcessor = new EventProcessorImpl<>(
                eventStore,
                transactionManager,
                new DefaultAggregateStore<>(eventStore, eventHandler, transactionManager),
                commandHandler.toCommandHandler(executor),
                eventHandler,
                List.empty()
        );
        this.eventProcessorVanilla = new EventProcessorVanilla<>(this.eventProcessor);
    }

    private TransactionManager<Transaction<BankEvent, Tuple0, Tuple0>> noOpTransactionManager() {
        return new TransactionManager<>() {
            @Override
            public <T> CompletionStage<T> withTransaction(Function<Transaction<BankEvent, Tuple0, Tuple0>, CompletionStage<T>> function) {
                return function.apply(new Transaction<>());
            }
        };
    }

    public CompletionStage<Either<String, ProcessingSuccess<Account, BankEvent, Tuple0, Tuple0, Tuple0>>> createAccount(
            BigDecimal amount) {
        Lazy<String> lazyId = Lazy.of(() -> UUIDgenerator.generate().toString());
        return eventProcessor.processCommand(new BankCommand.OpenAccount(lazyId, amount));
    }

    public CompletionStage<Either<String, ProcessingSuccess<Account, BankEvent, Tuple0, Tuple0, Tuple0>>> withdraw(
            String account, BigDecimal amount) {
        return eventProcessor.processCommand(new BankCommand.Withdraw(account, amount));
    }

    public CompletionStage<Either<String, ProcessingSuccess<Account, BankEvent, Tuple0, Tuple0, Tuple0>>> deposit(
            String account, BigDecimal amount) {
        return eventProcessor.processCommand(new BankCommand.Deposit(account, amount));
    }

    public CompletionStage<Either<String, ProcessingSuccess<Account, BankEvent, Tuple0, Tuple0, Tuple0>>> close(
            String account) {
        return eventProcessor.processCommand(new BankCommand.CloseAccount(account));
    }

    public CompletionStage<Option<Account>> findAccountById(String id) {
        return eventProcessor.getAggregate(id);
    }
}
