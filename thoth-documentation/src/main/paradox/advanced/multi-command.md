# Multiple command handling in the same transaction

To process multiple commands at once, you should use `batchProcessCommand` method.
Commands will be process in the same transaction, therefore if one command fail, events derived from the other won't be written / published.

Code below show implementation of multiple command processing to transfer money from one account to another in the same transaction, [see complete code here](https://github.com/MAIF/thoth/blob/master/sample/src/main/java/fr/maif/thoth/sample/api/BankController.java).

```java
eventProcessor.batchProcessCommand(List.of(
        new BankCommand.Withdraw(transferDTO.from, transferDTO.amount),
        new BankCommand.Deposit(transferDTO.to, transferDTO.amount))
    )
    .map(Either::sequence)
    .map(either -> either.fold(
        errors -> new ResponseEntity<TransferResultDTO>(TransferResultDTO.error(String.join("\n", errors)), HttpStatus.BAD_REQUEST),
        success -> {
            var fromResult = success.get(0);
            var toResult = success.get(1);
    
            return ResponseEntity.ok(new TransferResultDTO(
            AccountDTO.fromAccount(fromResult.currentState.get()),
            AccountDTO.fromAccount(toResult.currentState.get())));
        }
    )
    );
```
