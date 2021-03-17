# Projections

Our account management system is limited in consultation : all we can do is read accounts by id, one by one.

Projections help us to implement different read scenario. They are built / updated by consuming events.

## In transaction projection

thoth offers tools for building "in transaction" projections.
These projections will be updated in the transaction used to register events in the database, therefore they'll be updated in "real time".

Let's say we want a projection that stores mean withdrawal value by month.

We could have this table: 

```sql
CREATE TABLE IF NOT EXISTS WITHDRAW_BY_MONTH(
    client_id text,
    month text,
    year smallint,
    withdraw numeric,
    count integer
);
CREATE UNIQUE INDEX IF NOT EXISTS WITHDRAW_BY_MONTH_UNIQUE_IDX ON WITHDRAW_BY_MONTH(client_id, month, year);
ALTER TABLE WITHDRAW_BY_MONTH ADD CONSTRAINT WITHDRAW_BY_MONTH_UNIQUE UNIQUE USING INDEX WITHDRAW_BY_MONTH_UNIQUE_IDX;
```


```java
public class WithdrawByMonthProjection implements Projection<PgAsyncTransaction, BankEvent, Tuple0, Tuple0> {

    private final PgAsyncPool pgAsyncPool;

    public MeanWithdrawProjection(PgAsyncPool pgAsyncPool) {
        this.pgAsyncPool = pgAsyncPool;
    }

    @Override
    public Future<Tuple0> storeProjection(PgAsyncTransaction connection, List<EventEnvelope<BankEvent, Tuple0, Tuple0>> envelopes) {
        return connection.executeBatch(dsl ->
                envelopes
                        // Keep only MoneyWithdrawn events
                        .collect(unlift(eventEnvelope ->
                            Match(eventEnvelope.event).option(
                                Case(BankEvent.$MoneyWithdrawn(), e -> Tuple(eventEnvelope, e))
                            )
                        ))
                        // Store withdraw by month
                        .map(t -> dsl.query("""
                                        insert into withdraw_by_month (client_id, month, year, withdraw, count) values (?, ?, ?, ?, 1)
                                        on conflict on constraint WITHDRAW_BY_MONTH_UNIQUE
                                        do update set withdraw = withdraw_by_month.withdraw + EXCLUDED.withdraw, count=withdraw_by_month.count + 1
                                    """,
                                    val(t._2.entityId()),
                                    val(t._1.emissionDate.getMonth().name()),
                                    val(t._1.emissionDate.getYear()),
                                    val(t._2.amount)
                        ))
            ).map(__ -> Tuple.empty());
    }

    public Future<BigDecimal> meanWithdrawByClientAndMonth(String clientId, Integer year, String month) {
        return pgAsyncPool.query(dsl -> dsl.resultQuery(
                """
                    select round(withdraw / count::decimal, 2) 
                    from withdraw_by_month 
                    where  client_id = {0} and year = {1} and month = {2}                   
                    """,
                val(clientId),
                val(year),
                val(month))
        ).map(r -> r.head().get(0, BigDecimal.class));
    }

    public Future<BigDecimal> meanWithdrawByClient(String clientId) {
        return pgAsyncPool.query(dsl -> dsl
                .resultQuery(
                """
                    select round(sum(withdraw) / sum(count)::decimal, 2) as sum
                    from withdraw_by_month 
                    where  client_id = {0}
                    """, val(clientId)
                )
        ).map(r -> r.head().get("sum", BigDecimal.class));
    }
}
```

In the above example, for each event we build an sql statement and we store the statement in batch on postgresql. 

Next step is to declare the projection in our EventProcessor implementation.

```java
public class Bank {
    private final WithdrawByMonthProjection withdrawByMonthProjection;
    //...
    public Bank() {
        //...
        this.withdrawByMonthProjection = new WithdrawByMonthProjection(pgAsyncPool);

        this.eventProcessor = ReactivePostgresKafkaEventProcessor
                .withSystem(actorSystem)
                .withPgAsyncPool(pgAsyncPool)
                .withTables(tableNames())
                .withTransactionManager()
                .withEventFormater(BankEventFormat.bankEventFormat.jacksonEventFormat())
                .withNoMetaFormater()
                .withNoContextFormater()
                .withKafkaSettings("bank", producerSettings(settings()))
                .withEventHandler(eventHandler)
                .withDefaultAggregateStore()
                .withCommandHandler(commandHandler)
                .withProjections(this.withdrawByMonthProjection)
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
		//...
		String id = bank.createAccount(BigDecimal.valueOf(100)).get().get().currentState.get().id;

		bank.withdraw(id, BigDecimal.valueOf(50)).get().get().currentState.get();
		bank.withdraw(id, BigDecimal.valueOf(10)).get().get().currentState.get();

		System.out.println(bank.meanWithdrawByClient(id).get()); // 60
	}
}
```

## Eventually consistent projections

Sometimes projections are too costly to be updated in transaction, sometimes we don't need real time update.

In these case we could build "eventually consistent" projections, by connecting to our "bank" topic in Kafka, and consuming events from there.

@ref:[Eventually consistent projections](../eventually-consistent-projection.md) show how Thoth can help you to build eventually consistent projections.

See @ref:[Kafka consumption section](../kafka-consumption.md) for more information on published events and kafka consumption.

## The end

Congratulations ! You've reached the end of our documentation.

Please submit an issue to suggest any improvement.
