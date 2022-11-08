# Warning / Info messages

In addition to event generation, command handlers can also return messages, that can be use to give info / warning to the caller.

For instance, let's say that we want to issue a warning message when a `Withdraw` command put account balance under 0.

```java
public class BankCommandHandler implements CommandHandler<String, Account, BankCommand, BankEvent, List<String>, Connection> {
    @Override
    public CompletionStage<Either<String, Events<BankEvent, List<String>>>> handleCommand(
            Connection transactionContext,
            Option<Account> previousState,
            BankCommand command) {
        return CompletableFuture.supplyAsync(() -> switch (command) {
            case BankCommand.Withdraw withdraw -> this.handleWithdraw(previousState, withdraw);
            case BankCommand.Deposit deposit -> this.handleDeposit(previousState, deposit);
            case BankCommand.OpenAccount openAccount -> this.handleOpening(openAccount);
            case BankCommand.CloseAccount close -> this.handleClosing(previousState, close);
        });
    }

    //...

    private Either<String, Events<BankEvent, List<String>>> handleWithdraw(
            Option<Account> previousState,
            BankCommand.Withdraw withdraw) {
        return previousState.toEither("Account does not exist")
            .flatMap(previous -> {
                BigDecimal newBalance = previous.balance.subtract(withdraw.amount);
                List<String> messages = List();
                if(newBalance.compareTo(BigDecimal.ZERO) < 0) {
                    messages = messages.push("Overdrawn account");
                }
                return Right(Events.events(messages, new BankEvent.MoneyWithdrawn(withdraw.account, withdraw.amount)));
            });
    }
}
```

We needed to replace `CommandHandler`'s `Tuple0` parameter by a `List<String>` well suited to represent basic messages.
This implies to change this `Message` parameter from `Tuple0` to `List<Message>` everywhere in the application.
