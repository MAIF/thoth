package com.example.demo;

import java.math.BigDecimal;
import java.sql.SQLException;

public class DemoApplication {

	public static void main(String[] args) throws SQLException {
		BankCommandHandler commandHandler = new BankCommandHandler();
		BankEventHandler eventHandler = new BankEventHandler();
		Bank bank = new Bank(commandHandler, eventHandler);

		String id = bank.createAccount(BigDecimal.valueOf(100)).toCompletableFuture().join().get().currentState.get().id;

		bank.withdraw(id, BigDecimal.valueOf(50)).toCompletableFuture().join().get().currentState.get();
		BigDecimal balance = bank.withdraw(id, BigDecimal.valueOf(10)).toCompletableFuture().join().get().currentState.get().balance;
		System.out.println(balance);

		System.out.println(bank.meanWithdrawValue());
	}

}
