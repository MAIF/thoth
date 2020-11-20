package com.example.demo;

import akka.actor.ActorSystem;
import fr.maif.eventsourcing.ProcessingSuccess;
import io.vavr.Tuple0;
import io.vavr.control.Either;

import java.math.BigDecimal;

import io.vavr.concurrent.Future;

import static io.vavr.API.*;

public class DemoApplication {

    public static void main(String[] args) {
        ActorSystem actorSystem = ActorSystem.create();
        BankCommandHandler commandHandler = new BankCommandHandler();
        BankEventHandler eventHandler = new BankEventHandler();
        Bank bank = new Bank(actorSystem, commandHandler, eventHandler);

        Future<Either<String, BigDecimal>> result = bank.init()
                .flatMap(__ -> bank.createAccount(BigDecimal.valueOf(100)))
                .flatMap(accountCreatedOrError ->
                        accountCreatedOrError
                                .flatMap(processingResult -> processingResult.currentState.toEither("Current state is missing"))
                                .fold(
                                        error -> Future(Either.<String, ProcessingSuccess<Account, BankEvent, Tuple0, Tuple0, Tuple0>>left(error)),
                                        currentState -> {
                                            String id = currentState.id;
                                            return bank.withdraw(id, BigDecimal.valueOf(50));
                                        }
                                )
                )
                .map(withDrawProcessingResult -> withDrawProcessingResult
                        .flatMap(pr -> pr.currentState.toEither("Balance missing"))
                        .map(Account::getBalance)
                );

        result
                .onSuccess(balanceOrError ->
                        balanceOrError
                                .peek(balance -> println(balance))
                                .orElseRun(error -> println("Error: " + error))
                )
                .onFailure(Throwable::printStackTrace)
                .onComplete(res -> actorSystem.terminate());
    }

}
