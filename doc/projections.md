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
    public Future<Tuple0> storeProjection(Connection connection, List<EventEnvelope<BankEvent, Tuple0, Tuple0>> envelopes) {
        return Future.of(() -> {
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
        this.eventProcessor = PostgresKafkaEventProcessor.create(
            actorSystem,
            eventStore(actorSystem, producerSettings, "bank", dataSource, executorService, new TableNames("bank_journal", "bank_sequence_num") ,eventFormat),
            new JdbcTransactionManager(dataSource(), Executors.newFixedThreadPool(5)),
            commandHandler,
            eventHandler,
            List.of(meanWithdrawProjection)
        );
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
		ActorSystem actorSystem = ActorSystem.create();
		BankCommandHandler commandHandler = new BankCommandHandler();
		BankEventHandler eventHandler = new BankEventHandler();
		Bank bank = new Bank(actorSystem, commandHandler, eventHandler);

		String id = bank.createAccount(BigDecimal.valueOf(100)).get().get().currentState.get().id;

		bank.withdraw(id, BigDecimal.valueOf(50)).get().get().currentState.get();
		bank.withdraw(id, BigDecimal.valueOf(10)).get().get().currentState.get();

		System.out.println(bank.meanWithdrawValue()); // 30
	}
}
```

## Eventually consistent projections

Sometimes projections are too costly to be updated in transaction, sometimes we don't need real time update.

In these case we could build "eventually consistent" projections, by connecting to our "bank" topic in Kafka, and consuming events from there.

## Next step

[Configure your database](./database%20configuration.md)