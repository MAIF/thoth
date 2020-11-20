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

        bank.init()
                .flatMap(__ -> bank.createAccount(BigDecimal.valueOf(100)))
                .flatMap(accountCreatedOrError ->
                        accountCreatedOrError
                                .fold(
                                        error -> Future(Either.<String, Account>left(error)),
                                        currentState -> {
                                            String id = currentState.id;
                                            println("account created with id "+id);
                                            return bank.withdraw(id, BigDecimal.valueOf(50))
                                                    .map(withDrawProcessingResult -> withDrawProcessingResult.map(Account::getBalance))
                                                    .onSuccess(balanceOrError ->
                                                            balanceOrError
                                                                    .peek(balance -> println("Balance is now: "+balance))
                                                                    .orElseRun(error -> println("Error: " + error))
                                                    )
                                                    .flatMap(balanceOrError ->
                                                            bank.deposit(id, BigDecimal.valueOf(100))
                                                    )
                                                    .map(depositProcessingResult -> depositProcessingResult.map(Account::getBalance))
                                                    .onSuccess(balanceOrError ->
                                                            balanceOrError
                                                                    .peek(balance -> println("Balance is now: "+balance))
                                                                    .orElseRun(error -> println("Error: " + error))
                                                    )
                                                    .flatMap(balanceOrError ->
                                                            bank.findAccountById(id)
                                                    )
                                                    .onSuccess(balanceOrError ->
                                                            balanceOrError.forEach(account -> println("Account is: "+account ))
                                                    );
                                        }
                                )
                )
                .onFailure(Throwable::printStackTrace)
                .onComplete(res -> {
                    Try(() -> {
                        bank.close();
                        return "";
                    });
                    actorSystem.terminate();
                });
    }

}
