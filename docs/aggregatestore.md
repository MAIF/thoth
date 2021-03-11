# Aggregate store

In the current state of our system, if we request an account, here is what happen:

* The system read all events related to our account id
* Starting from an empty state, it applies all events to "rebuild" the latest state version
* It returns this latest state

In a real world bank system, we would have several thousands of events.

In such a scenario, applying all events would have a high performance cost.

To fix this issue, one solution is to implement an `AggregateStore`.

```java
public class BankAggregateStore extends DefaultAggregateStore<Account, BankEvent, Tuple0, Tuple0, Connection> {

    public BankAggregateStore(EventStore<Connection, BankEvent, Tuple0, Tuple0> eventStore, EventHandler<Account, BankEvent> eventEventHandler, ActorSystem system, TransactionManager<Connection> transactionManager) {
        super(eventStore, eventEventHandler, system, transactionManager);
    }

    public BankAggregateStore(EventStore<Connection, BankEvent, Tuple0, Tuple0> eventStore, EventHandler<Account, BankEvent> eventEventHandler, Materializer materializer, TransactionManager<Connection> transactionManager) {
        super(eventStore, eventEventHandler, materializer, transactionManager);
    }

    @Override
    public Future<Tuple0> storeSnapshot(
            Connection connection,
            String id,
            Option<Account> maybeState) {
        return Future.of(() -> {

            maybeState.peek(state -> {
                try {
                    PreparedStatement statement = connection.prepareStatement("""
                        INSERT INTO ACCOUNTS(ID, BALANCE) VALUES(?, ?)
                        ON CONFLICT (id) DO UPDATE SET balance = ?
                    """);
                    statement.setString(1, id);
                    statement.setBigDecimal(2, state.balance);
                    statement.setBigDecimal(3, state.balance);
                    statement.execute();
                } catch (SQLException throwable) {
                    throw new RuntimeException(throwable);
                }
            });

            return Tuple.empty();
        });
    }

    @Override
    public Future<Option<Account>> getAggregate(Connection connection, String entityId) {
        return Future.of(() -> {
                PreparedStatement statement = connection.prepareStatement("SELECT balance FROM ACCOUNTS WHERE id=?");
                statement.setString(1, entityId);
                ResultSet resultSet = statement.executeQuery();

                if(resultSet.next()) {
                    BigDecimal amount = resultSet.getBigDecimal("balance");

                    Account account = new Account();
                    account.id = entityId;
                    account.balance = amount;

                    return Option.some(account);
                } else {
                    return Option.none();
                }
        });
    }
}
```

This aggregate store maintains a parallel `Accounts` table in the database that stores the latest state of each account.

To add this aggregateStore in our system, we need to pass it to our `PostgresKafkaEventProcessor`.

```java
public class Bank {
    //...
    public Bank() {
        //...
        BankAggregateStore bankAggregateStore = new BankAggregateStore(eventStore, eventHandler, actorSystem, transactionManager);

        this.eventProcessor = new PostgresKafkaEventProcessor<>(new PostgresKafkaEventProcessor.PostgresKafkaEventProcessorConfig<>(
                eventStore,
                transactionManager,
                bankAggregateStore,
                commandHandler,
                eventHandler,
                List.of(meanWithdrawProjection),
                kafkaEventPublisher
        ));
    }
    //...
}
```

*IMPORTANT NOTE*: This aggregate store should only be used when having performance issues on state reading.
It's a tradeoff where we accept to have slower write to get faster read. 

To implement an alternative read model (i.e. CQRS), [projections](./projections.md) should be used.

## Next step

[Non blocking Postgres / Kafka implementation](./banking-real-life-non-blocking.md)
