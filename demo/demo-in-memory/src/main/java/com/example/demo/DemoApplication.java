package com.example.demo;

import java.math.BigDecimal;
import java.util.Objects;

public class DemoApplication {

	public static void main(String[] args) {
		BankCommandHandler commandHandler = new BankCommandHandler();
		BankEventHandler eventHandler = new BankEventHandler();
		Bank bank = new Bank(commandHandler, eventHandler);

		String id = bank.createAccount(BigDecimal.valueOf(100))
				.whenComplete((either, e) -> {
					if (Objects.isNull(e)) {
						either.map(result -> result.currentState
										.peek(account -> System.out.println(account.balance))
								)
								.peekLeft(System.err::println);
					} else {
						e.printStackTrace();
					}
				})
				.toCompletableFuture().join().get().currentState.get().id;

		BigDecimal balance = bank.withdraw(id, BigDecimal.valueOf(50)).toCompletableFuture().join().get().currentState.get().balance;
		System.out.println(balance);
	}

}
