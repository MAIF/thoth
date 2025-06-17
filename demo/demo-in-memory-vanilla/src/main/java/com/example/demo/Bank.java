package com.example.demo;

import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.impl.TimeBasedGenerator;
import fr.maif.eventsourcing.Lazy;
import fr.maif.eventsourcing.Result;
import fr.maif.eventsourcing.TransactionManager;
import fr.maif.eventsourcing.Unit;
import fr.maif.eventsourcing.vanilla.*;
import fr.maif.reactor.eventsourcing.vanilla.DefaultAggregateStore;
import fr.maif.reactor.eventsourcing.vanilla.InMemoryEventStore;
import fr.maif.reactor.eventsourcing.vanilla.InMemoryEventStore.Transaction;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

public class Bank {
    private final EventProcessor<String, Account, BankCommand, BankEvent, Transaction<BankEvent, Unit, Unit>, Unit, Unit, Unit> eventProcessor;
    private static final TimeBasedGenerator UUIDgenerator = Generators.timeBasedGenerator();


    public Bank(
                BankCommandHandler commandHandler,
                BankEventHandler eventHandler) {

        EventStore<Transaction<BankEvent, Unit, Unit>, BankEvent, Unit, Unit> eventStore = InMemoryEventStore.create();
        TransactionManager<Transaction<BankEvent, Unit, Unit>> transactionManager = noOpTransactionManager();
        ExecutorService executor = Executors.newCachedThreadPool();
        DefaultAggregateStore<Account, BankEvent, Unit, Unit, Transaction<BankEvent, Unit, Unit>> aggregateStore = new DefaultAggregateStore<>(eventStore, eventHandler, transactionManager);
        this.eventProcessor = new EventProcessorImpl<>(
                eventStore,
                transactionManager,
                aggregateStore,
                commandHandler.toCommandHandler(executor),
                eventHandler,
                List.of()
        );
    }

    private TransactionManager<Transaction<BankEvent, Unit, Unit>> noOpTransactionManager() {
        return new TransactionManager<>() {
            @Override
            public <T> CompletionStage<T> withTransaction(Function<Transaction<BankEvent, Unit, Unit>, CompletionStage<T>> function) {
                return function.apply(new Transaction<>());
            }
        };
    }

    public CompletionStage<Result<String, ProcessingSuccess<Account, BankEvent, Unit, Unit, Unit>>> createAccount(
            BigDecimal amount) {
        Lazy<String> lazyId = Lazy.of(() -> UUIDgenerator.generate().toString());
        return eventProcessor.processCommand(new BankCommand.OpenAccount(lazyId, amount));
    }

    public CompletionStage<Result<String, ProcessingSuccess<Account, BankEvent, Unit, Unit, Unit>>> withdraw(
            String account, BigDecimal amount) {
        return eventProcessor.processCommand(new BankCommand.Withdraw(account, amount));
    }

    public CompletionStage<Result<String, ProcessingSuccess<Account, BankEvent, Unit, Unit, Unit>>> deposit(
            String account, BigDecimal amount) {
        return eventProcessor.processCommand(new BankCommand.Deposit(account, amount));
    }

    public CompletionStage<Result<String, ProcessingSuccess<Account, BankEvent, Unit, Unit, Unit>>> close(
            String account) {
        return eventProcessor.processCommand(new BankCommand.CloseAccount(account));
    }

    public CompletionStage<Optional<Account>> findAccountById(String id) {
        return eventProcessor.getAggregate(id);
    }
}
