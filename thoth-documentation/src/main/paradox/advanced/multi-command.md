# Multiple command handling in the same transaction

To send multiple commands in the same transaction, transaction context should be instantiated manually and passed to command handler along with commands.

Code below show implementation of multiple command processing to transfer money from one account to another in the same transaction, [see complete code here](https://github.com/MAIF/thoth/blob/master/sample/src/main/java/fr/maif/thoth/sample/api/BankController.java).

```java
try(Connection connection = dataSource.getConnection()) {
    connection.setAutoCommit(false);
    var withdrawResult = eventProcessor
            .batchProcessCommand(
                    connection,
                    List.of(new BankCommand.Withdraw(transferDTO.from, transferDTO.amount))
            );
    var depositResult = eventProcessor
            .batchProcessCommand(
                    connection,
                    List.of(new BankCommand.Deposit(transferDTO.to, transferDTO.amount))
            );

    return withdrawResult.zip(depositResult).map(tuple ->
        tuple._1.and(tuple._2, (leither1, leither2) -> {
            var mergedResult = Either
                    .sequence(List.of(leither1.head(), leither2.head()));
            try {
                if(mergedResult.isLeft()) {
                    connection.rollback();
                } else {
                    connection.commit();
                }
            } catch (SQLException exception) {
                return new ResponseEntity<>(TransferResultDTO.error("Failed to commit / rollback"), HttpStatus.INTERNAL_SERVER_ERROR);
            }
            return transferResultToDTO(mergedResult, transferDTO.from, transferDTO.to);
        }

    )).flatMap(TransactionManager.InTransactionResult::postTransaction)
    .toCompletableFuture().join();

}
```
