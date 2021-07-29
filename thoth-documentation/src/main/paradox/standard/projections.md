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


### Catch up past events

In some cases, the projection will be created while events already exist in the journal.
Sometimes these pre-existing events should be added to the projection.

To "catch up" with past events, you need to stream journal content:

```java
eventStore.loadAllEvents()
    .map(enveloppe -> enveloppe.event)
    .filter(event -> event instanceof BankEvent.MoneyDeposited || event instanceof BankEvent.MoneyWithdrawn)
    .mapAsync(1, event ->
        CompletableFuture.supplyAsync(() -> {
            try {
                if(event instanceof BankEvent.MoneyDeposited deposit) {
                    String statement = "UPDATE global_balance SET balance=balance+?::money";
                    try(PreparedStatement preparedStatement = connection.prepareStatement(statement)) {
                        preparedStatement.setBigDecimal(1, deposit.amount);
                        preparedStatement.execute();
                    }
                } else if(event instanceof BankEvent.MoneyWithdrawn withdraw) {
                    String statement = "UPDATE global_balance SET balance=balance-?::money";
                    try(PreparedStatement preparedStatement = connection.prepareStatement(statement)) {
                        preparedStatement.setBigDecimal(1, withdraw.amount);
                        preparedStatement.execute();
                    }
                }
                return Tuple.empty();
            } catch(SQLException ex) {
                throw new RuntimeException(ex);
            }
        })
    ).run(actorSystem)
```

⚠️ This code should be run while the system is not receiving events, otherwise there is a risk of double consumption.
To fix this one solution would be to store consumed event id or sequence num and compare them with incoming events.

## Eventually consistent projections

Sometimes projections are too costly to be updated in transaction, sometimes we don't need real time update.

In these case we could build "eventually consistent" projections, by connecting to our "bank" topic in Kafka, and consuming events from there.

@ref:[Eventually consistent projections](../eventually-consistent-projection.md) show how Thoth can help you to build eventually consistent projections.

See @ref:[Kafka consumption section](../kafka-consumption.md) for more information on published events and kafka consumption.
