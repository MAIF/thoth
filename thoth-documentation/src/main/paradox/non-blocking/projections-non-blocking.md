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
public class WithdrawByMonthProjection implements ReactorProjection<PgAsyncTransaction, BankEvent, Tuple0, Tuple0> {

    private final PgAsyncPool pgAsyncPool;

    public WithdrawByMonthProjection(PgAsyncPool pgAsyncPool) {
        this.pgAsyncPool = pgAsyncPool;
    }

    @Override
    public Mono<Tuple0> storeProjection(PgAsyncTransaction connection, List<EventEnvelope<BankEvent, Tuple0, Tuple0>> envelopes) {
        return connection.executeBatchMono(dsl ->
                envelopes
                        // Keep only MoneyWithdrawn events
                        .collect(unlift(eventEnvelope ->
                                switch (eventEnvelope.event) {
                                    case BankEvent.MoneyWithdrawn e -> Some(Tuple(eventEnvelope, e));
                                    default -> None();
                                }
                        ))
                        // Store withdraw by month
                        .map(t -> dsl.query("""
                                            insert into withdraw_by_month (client_id, month, year, withdraw, count) values ({0}, {1}, {2}, {3}, 1)
                                            on conflict on constraint WITHDRAW_BY_MONTH_UNIQUE
                                            do update set withdraw = withdraw_by_month.withdraw + EXCLUDED.withdraw, count=withdraw_by_month.count + 1
                                        """,
                                val(t._2.entityId()),
                                val(t._1.emissionDate.getMonth().name()),
                                val(t._1.emissionDate.getYear()),
                                val(t._2.amount())
                        ))
        ).thenReturn(Tuple.empty());
    }

    public Mono<BigDecimal> meanWithdrawByClientAndMonth(String clientId, Integer year, String month) {
        return pgAsyncPool.queryMono(dsl -> dsl.resultQuery(
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

    public Mono<BigDecimal> meanWithdrawByClient(String clientId) {
        return pgAsyncPool.queryMono(dsl -> dsl
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
    public Bank(BankCommandHandler commandHandler, BankEventHandler eventHandler) {
        this.vertx = Vertx.vertx();
        this.pgAsyncPool = pgAsyncPool(vertx);
        this.withdrawByMonthProjection = new WithdrawByMonthProjection(pgAsyncPool);

        this.eventProcessor = ReactiveEventProcessor
                .withPgAsyncPool(pgAsyncPool)
                .withTables(tableNames())
                .withTransactionManager()
                .withEventFormater(BankEventFormat.bankEventFormat.jacksonEventFormat())
                .withNoMetaFormater()
                .withNoContextFormater()
                .withKafkaSettings("bank", senderOptions(settings()))
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
BankCommandHandler commandHandler = new BankCommandHandler();
BankEventHandler eventHandler = new BankEventHandler();
Bank bank = new Bank(commandHandler, eventHandler);

bank.init()
        .flatMap(__ -> bank.createAccount(BigDecimal.valueOf(100)))
        .flatMap(accountCreatedOrError ->
                accountCreatedOrError
                        .fold(
                                error -> Mono.just(Either.<String, Account>left(error)),
                                currentState -> {
                                    String id = currentState.id;
                                    println("account created with id "+id);
                                    return bank.withdraw(id, BigDecimal.valueOf(50))
                                            .map(withDrawProcessingResult -> withDrawProcessingResult.map(Account::getBalance))
                                            .doOnSuccess(balanceOrError ->
                                                    balanceOrError
                                                            .peek(balance -> println("Balance is now: "+balance))
                                                            .orElseRun(error -> println("Error: " + error))
                                            )
                                            .flatMap(balanceOrError ->
                                                    bank.deposit(id, BigDecimal.valueOf(100))
                                            )
                                            .map(depositProcessingResult -> depositProcessingResult.map(Account::getBalance))
                                            .doOnSuccess(balanceOrError ->
                                                    balanceOrError
                                                            .peek(balance -> println("Balance is now: "+balance))
                                                            .orElseRun(error -> println("Error: " + error))
                                            )
                                            .flatMap(balanceOrError ->
                                                    bank.findAccountById(id)
                                            )
                                            .doOnSuccess(balanceOrError ->
                                                    balanceOrError.forEach(account -> println("Account is: "+account ))
                                            )
                                            .flatMap(__ ->
                                                    bank.withdraw(id, BigDecimal.valueOf(25))
                                            )
                                            .flatMap(__ ->
                                                bank.meanWithdrawByClient(id).doOnSuccess(w -> {
                                                    println("Withdraw sum "+w);
                                                })
                                            );
                                }
                        )
        )
        .doOnError(Throwable::printStackTrace)
        .doOnTerminate(() -> {
            Try(() -> {
                bank.close();
                return "";
            });
        })
        .subscribe();
```
## Eventually consistent projections

Sometimes projections are too costly to be updated in transaction, sometimes we don't need real time update.

In these case we could build "eventually consistent" projections, by connecting to our "bank" topic in Kafka, and consuming events from there.

@ref:[Eventually consistent projections](../eventually-consistent-projection.md) show how Thoth can help you to build eventually consistent projections.

See @ref:[Kafka consumption section](../kafka-consumption.md) for more information on published events and kafka consumption.

## The end

Congratulations ! You've reached the end of our documentation.

Please submit an issue to suggest any improvement.
