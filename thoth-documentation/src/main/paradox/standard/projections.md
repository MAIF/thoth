# Projections

Out account management system is limited in consultation : all we can do is read accounts by id, one by one.

Projections help us to implement different read scenario. They are built / updated by consuming events.

## In transaction projection

thoth offers tools for building "in transaction" projections.
These projections will be updated in the transaction used to register events in the database, therefore they'll be updated in "real time".

Let's say we want a projection that stores mean withdrawal value.


```java
public class MeanBalanceProjection implements Projection<Connection, BankEvent, Tuple0, Tuple0> {
    private BigDecimal withDrawTotal = BigDecimal.ZERO;
    private long withdrawCount = 0L;

    @Override
    public CompletionStage<Tuple0> storeProjection(Connection connection, List<EventEnvelope<BankEvent, Tuple0, Tuple0>> envelopes) {
        return CompletableFuture.supplyAsync(() -> {
            envelopes.forEach(envelope -> {
                BankEvent bankEvent = envelope.event;
                if(envelope.event instanceof BankEvent.MoneyWithdrawn) {
                    withDrawTotal = withDrawTotal.add(((BankEvent.MoneyWithdrawn)bankEvent).amount);
                    withdrawCount ++;
                }
            });
            
            return Tuple.empty();
        });
    }
    
    public BigDecimal meanWithdraw() {
        return withDrawTotal.divide(BigDecimal.valueOf(withdrawCount));
    }
}
```

In the above example, we implemented an in memory projection, however in a real use case we should store values somewhere (e.g. in a database).

Next step is to declare the projection in our EventProcessor implementation.

```java
public class Bank {
    private final MeanWithdrawProjection meanWithdrawProjection;
    //...
    public Bank() {
        //...

        this.meanWithdrawProjection = new MeanWithdrawProjection();

        ExecutorService executorService = Executors.newFixedThreadPool(5);
        JdbcTransactionManager transactionManager = new JdbcTransactionManager(dataSource(), executorService);

        this.eventProcessor = PostgresKafkaEventProcessor
                .withDataSource(dataSource())
                .withTables(tableNames)
                .withTransactionManager(transactionManager, executorService)
                .withEventFormater(eventFormat)
                .withNoMetaFormater()
                .withNoContextFormater()
                .withKafkaSettings(topic, producerSettings)
                .withEventHandler(eventHandler)
                .withAggregateStore(builder -> new BankAggregateStore(
                        builder.eventStore,
                        builder.eventHandler,
                        builder.transactionManager
                ))
                .withCommandHandler(commandHandler)
                .withProjections(meanWithdrawProjection)
                .build();
    }
    //...
    public BigDecimal meanWithdrawValue() {
        return meanWithdrawProjection.meanWithdraw();
    }
}
```

### Usage

```java
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
```

## Eventually consistent projections

Sometimes projections are too costly to be updated in transaction, sometimes we don't need real time update.

In these case we could build "eventually consistent" projections, by connecting to our "bank" topic in Kafka, and consuming events from there.

@ref:[Eventually consistent projections](../eventually-consistent-projection.md) show how Thoth can help you to build eventually consistent projections.

See @ref:[Kafka consumption section](../kafka-consumption.md) for more information on published events and kafka consumption.
