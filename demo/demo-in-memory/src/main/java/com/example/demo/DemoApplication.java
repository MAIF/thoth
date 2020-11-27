package com.example.demo;

import akka.actor.ActorSystem;
import io.vavr.concurrent.Future;
import io.vavr.control.Option;

import java.math.BigDecimal;

public class DemoApplication {

	public static void main(String[] args) {
		ActorSystem actorSystem = ActorSystem.create();
		BankCommandHandler commandHandler = new BankCommandHandler();
		BankEventHandler eventHandler = new BankEventHandler();
		Bank bank = new Bank(actorSystem, commandHandler, eventHandler);

		String id = bank.createAccount(BigDecimal.valueOf(100)).get().get().currentState.get().id;

		BigDecimal balance = bank.withdraw(id, BigDecimal.valueOf(50)).get().get().currentState.get().balance;
		System.out.println(balance);
	}

}
