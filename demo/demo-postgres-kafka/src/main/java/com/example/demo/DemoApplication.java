package com.example.demo;

import fr.maif.eventsourcing.Result;
import fr.maif.eventsourcing.Unit;
import fr.maif.eventsourcing.vanilla.ProcessingSuccess;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.List;

public class DemoApplication {

	public static void main(String[] args) throws SQLException {
		BankCommandHandler commandHandler = new BankCommandHandler();
		BankEventHandler eventHandler = new BankEventHandler();
		Bank bank = new Bank(commandHandler, eventHandler);

		String id = bank.createAccount(BigDecimal.valueOf(100)).toCompletableFuture().join().get().currentState.get().id;

		Result<String, ProcessingSuccess<Account, BankEvent, Unit, Unit, List<String>>> withdraw1 = bank.withdraw(id, BigDecimal.valueOf(50)).toCompletableFuture().join();

		Result<String, ProcessingSuccess<Account, BankEvent, Unit, Unit, List<String>>> withdraw2 = bank.withdraw(id, BigDecimal.valueOf(10)).toCompletableFuture().join();
		BigDecimal balance = withdraw2.get().currentState.get().balance;

		System.out.println(balance);

		System.out.println(bank.meanWithdrawValue());
	}

}
