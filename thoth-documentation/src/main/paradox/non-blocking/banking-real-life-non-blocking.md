# Postgres Kafka, non blocking event sourcing

This example is based on @ref:[bank example](../banking.md), we'll replace our InMemoryEventStore by a reactive Event store using Postgres and Kafka.

First we need to import `thoth-jooq-async` module. This module contains an implementation of thoth for Postgres using Jooq with the [vertx postgresql client](https://github.com/eclipse-vertx/vertx-sql-client).

## SQL

First thing first : we need to set up database.

Database and user creation: 

```sql
CREATE DATABASE eventsourcing;
CREATE USER eventsourcing WITH PASSWORD 'eventsourcing';
GRANT ALL PRIVILEGES ON DATABASE "eventsourcing" to eventsourcing;
```

Schema creation:

```sql
CREATE TABLE IF NOT EXISTS ACCOUNTS (
    id varchar(100) PRIMARY KEY,
    balance money NOT NULL
);

CREATE TABLE IF NOT EXISTS bank_journal (
  id UUID primary key,
  entity_id varchar(100) not null,
  sequence_num bigint not null,
  event_type varchar(100) not null,
  version int not null,
  transaction_id varchar(100) not null,
  event jsonb not null,
  metadata jsonb,
  context jsonb,
  total_message_in_transaction int default 1,
  num_message_in_transaction int default 1,
  emission_date timestamp not null default now(),
  user_id varchar(100),
  system_id varchar(100),
  published boolean default false,
  UNIQUE (entity_id, sequence_num)
);

CREATE SEQUENCE if not exists bank_sequence_num;
```

Here is what we need in the database:

* An `ACCOUNTS` table to keep our accounts safe, we kept it simple here to match our model
* A `BANK_JOURNAL` table that will contain our events
* A `BANK_SEQUENCE_NUM` to generate sequence_num of our events

# Code

First of all let's swap `thoth-core-async` dependency with `thoth-jooq-async`. This new dependency provides everything we need to set up postgres / kafka connection.

@@dependency[sbt,Maven,Gradle] {
symbol="ThothVersion"
value="$project.version.short$"
group="fr.maif" artifact="thoth-jooq-async" version="ThothVersion"
}


## Event serialization

Let's start with event reading and writing. We need to declare a serializer to read / write events to DB.
This time we will use `JsonRead`and `JsonWrite` from [functionnal-json](https://github.com/MAIF/functional-json). 

```java

public class BankEventFormat implements EventEnvelopeJsonFormat<BankEvent, Tuple0, Tuple0> {

    public static BankEventFormat bankEventFormat = new BankEventFormat();

    @Override
    public List<Tuple2<Type<? extends BankEvent>, JsonRead<? extends BankEvent>>> cases() {
        return List(
                Tuple(MoneyWithdrawnV1, MoneyWithdrawn.format),
                Tuple(AccountOpenedV1, AccountOpened.format),
                Tuple(MoneyDepositedV1, MoneyDeposited.format),
                Tuple(AccountClosedV1, AccountClosed.format)
        );
    }

    @Override
    public JsonWrite<BankEvent> eventWrite() {
        return BankEvent.format;
    }
}
```

We implemented this using [functionnal-json](https://github.com/MAIF/functional-json) library, since it provides nice utilities to handle / aggregate deserialization errors.

Now we have to write readers and writers for each `BankEvent` 

```java
public sealed interface BankEvent extends Event {
    Type<MoneyWithdrawn> MoneyWithdrawnV1 = Type.create(MoneyWithdrawn.class, 1L);
    Type<AccountOpened> AccountOpenedV1 = Type.create(AccountOpened.class, 1L);
    Type<MoneyDeposited> MoneyDepositedV1 = Type.create(MoneyDeposited.class, 1L);
    Type<AccountClosed> AccountClosedV1 = Type.create(AccountClosed.class, 1L);

    JsonFormat<BankEvent> format = JsonFormat.of(
            JsonRead.oneOf(_string("type"),
                    caseOf("MoneyWithdrawn"::equals, MoneyWithdrawn.format),
                    caseOf("AccountOpened"::equals, AccountOpened.format),
                    caseOf("MoneyDeposited"::equals, MoneyDeposited.format),
                    caseOf("AccountClosed"::equals, AccountClosed.format)
            ),
            (BankEvent event) -> switch (event) {
                case MoneyWithdrawn bankEvent -> MoneyWithdrawn.format.write(bankEvent);
                case AccountOpened bankEvent -> AccountOpened.format.write(bankEvent);
                case MoneyDeposited bankEvent -> MoneyDeposited.format.write(bankEvent);
                case AccountClosed bankEvent -> AccountClosed.format.write(bankEvent);
            }
    );

    record MoneyWithdrawn(String accountId, BigDecimal amount) implements BankEvent {

        static class MoneyWithdrawnBuilder{
            String accountId;
            BigDecimal amount;

            MoneyWithdrawnBuilder accountId(String accountId){
                this.accountId = accountId;
                return this;
            }

            MoneyWithdrawnBuilder amount(BigDecimal amount){
                this.amount = amount;
                return this;
            }

            MoneyWithdrawn build(){
                return new MoneyWithdrawn(accountId,amount);
            }

        }

        public static MoneyWithdrawnBuilder builder(){
            return new MoneyWithdrawnBuilder();
        }

        @Override
        public Type<MoneyWithdrawn> type() {
            return MoneyWithdrawnV1;
        }

        @Override
        public String entityId() {
            return accountId;
        }

        public static JsonFormat<MoneyWithdrawn> format = JsonFormat.of(
                __("amount", _bigDecimal(), MoneyWithdrawn.builder()::amount)
                        .and(_string("accountId"), MoneyWithdrawn.MoneyWithdrawnBuilder::accountId)
                        .map(MoneyWithdrawn.MoneyWithdrawnBuilder::build),
                (MoneyWithdrawn moneyWithdrawn) -> Json.obj(
                        $$("type", "MoneyWithdrawn"),
                        $$("amount", moneyWithdrawn.amount, $bigdecimal()),
                        $$("accountId", moneyWithdrawn.accountId)
                )
        );
    }

    record AccountOpened(String accountId) implements BankEvent {
        static class AccountOpenedBuilder {
            String accountId;

            AccountOpenedBuilder accountId(String accountId){
                this.accountId = accountId;
                return this;
            }

            AccountOpened build(){
                return new AccountOpened(accountId);
            }
        }

        public static AccountOpenedBuilder builder(){
            return new AccountOpenedBuilder();
        }

        @Override
        public Type<AccountOpened> type() {
            return AccountOpenedV1;
        }

        @Override
        public String entityId() {
            return accountId;
        }

        public static JsonFormat<AccountOpened> format = JsonFormat.of(
                __("accountId", _string(), AccountOpened.AccountOpened.builder()::accountId)
                        .map(AccountOpened.AccountOpenedBuilder::build),
                (AccountOpened accountOpened) -> Json.obj(
                        $$("type", "AccountOpened"),
                        $$("accountId", accountOpened.accountId)
                )
        );
    }


    record MoneyDeposited(String accountId, BigDecimal amount) implements BankEvent {

        static class MoneyDepositedBuilder {
            String accountId;
            BigDecimal amount;

            MoneyDepositedBuilder accountId(String accountId){
                this.accountId = accountId;
                return this;
            }

            MoneyDepositedBuilder amount(BigDecimal amount){
                this.amount = amount;
                return this;
            }

            MoneyDeposited build(){
                return new MoneyDeposited(accountId,amount);
            }

        }

        public static MoneyDepositedBuilder builder(){
            return new MoneyDepositedBuilder();
        }

        @Override
        public Type<MoneyDeposited> type() {
            return MoneyDepositedV1;
        }

        @Override
        public String entityId() {
            return accountId;
        }

        public static JsonFormat<MoneyDeposited> format = JsonFormat.of(
                __("accountId", _string(), MoneyDeposited.MoneyDeposited.builder()::accountId)
                        .and(__("amount", _bigDecimal()), MoneyDeposited.MoneyDepositedBuilder::amount)
                        .map(MoneyDeposited.MoneyDepositedBuilder::build),
                (MoneyDeposited moneyDeposited) -> Json.obj(
                        $$("type", "MoneyDeposited"),
                        $$("amount", moneyDeposited.amount, $bigdecimal()),
                        $$("accountId", moneyDeposited.accountId)
                )
        );
    }

    record AccountClosed(String accountId) implements BankEvent {
        static class AccountClosedBuilder{
            String accountId;

            AccountClosedBuilder accountId(String accountId){
                this.accountId = accountId;
                return this;
            }

            AccountClosed build(){
                return new AccountClosed(accountId);
            }
        }

        public static AccountClosedBuilder builder(){
            return new AccountClosedBuilder();
        }

        @Override
        public Type<AccountClosed> type() {
            return AccountClosedV1;
        }

        @Override
        public String entityId() {
            return accountId;
        }


        public static JsonFormat<AccountClosed> format = JsonFormat.of(
                __("accountId", _string(), AccountClosed.AccountClosed.builder()::accountId)
                        .map(AccountClosed.AccountClosedBuilder::build),
                (AccountClosed accountClosed) -> Json.obj(
                        $$("type", "AccountClosed"),
                        $$("accountId", accountClosed.accountId)
                )
        );
    }
}
```

## Database connection

Speaking of database, we also need to set up a database connection somewhere.

In the sample application, this is made in `Bank` class, in real world application, this could be made in some configuration class.

```Java
public class Bank {
    // ...
    private PgAsyncPool pgAsyncPool(Vertx vertx) {
        DefaultConfiguration jooqConfig = new DefaultConfiguration();
        jooqConfig.setSQLDialect(SQLDialect.POSTGRES);

        PgConnectOptions options = new PgConnectOptions()
                .setPort(5432)
                .setHost("localhost")
                .setDatabase("eventsourcing")
                .setUser("eventsourcing")
                .setPassword("eventsourcing");
        PoolOptions poolOptions = new PoolOptions().setMaxSize(50);
        PgPool pgPool = PgPool.pool(vertx, options, poolOptions);

        return PgAsyncPool.create(pgPool, jooqConfig);
    }
    // ...
}
```

We also need a `TableNames` instance to provide information about created table name and sequence.

```java
public class Bank {
    //...
    private TableNames tableNames() {
        return new TableNames("bank_journal", "bank_sequence_num");
    }
    //...
}
```

Since this implementation will use a real database, we need to change TransactionContext type from `Tuple0` to `Connection` in `CommandHandler`.

This transaction context allows sharing database context for command verification and events insertion.

```java
public class BankCommandHandler implements CommandHandler<String, Account, BankCommand, BankEvent, Tuple0, PgAsyncTransaction> {
    //...
}
```

## Kafka connection

To handle the kafka part, we need two things:
* A `KafkaSettings` instance, that should contain kafka location and keystore / truststore information (if needed)
* A `ProducerSettings` instance that will be used to publish events in kafka

```java
public class Bank {
    //...
    private KafkaSettings settings() {
        return KafkaSettings.newBuilder("localhost:29092").build();
    }

    private SenderOptions<String, EventEnvelope<BankEvent, Tuple0, Tuple0>> senderOptions(KafkaSettings kafkaSettings) {
        return kafkaSettings.producerSettings(JsonFormatSerDer.of(BankEventFormat.bankEventFormat));
    }
    //...
}
```


## Event processor

The next step is to swap our `EventProcessor` with `ReactivePostgresKafkaEventProcessor`.
This configuration is not trivial but we only have to do this once ! 
```java
public class Bank {
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
                .build();
    }
    //...
}
```

## Usage

Usage remains the same as in @ref:[in memory example](../banking.md).

A [docker-compose.yml](https://github.com/MAIF/thoth/tree/master/docker-compose.yml) file is available to set-up dev environment.

It exposes a PostgreSQL server on http://localhost:5432/ and a kafdrop instance on http://localhost:9000/.


```java
BankCommandHandler commandHandler = new BankCommandHandler();
BankEventHandler eventHandler = new BankEventHandler();
Bank bank = new Bank(commandHandler, eventHandler);
String id = bank.createAccount(BigDecimal.valueOf(100)).toCompletableFuture().join().get().currentState.get().id;

bank.withdraw(id, BigDecimal.valueOf(50)).toCompletableFuture().join().get().currentState.get();
```

The above code puts the following events in bank_journal table in postgres :

```
eventsourcing=> select * from bank_journal;
                  id                  |              entity_id               | sequence_num |   event_type   | version |            transaction_id            |                                event                                 | metadata | context | total_message_in_transaction | num_message_in_transaction |       emission_date        | user_id | system_id | published
--------------------------------------+--------------------------------------+--------------+----------------+---------+--------------------------------------+----------------------------------------------------------------------+----------+---------+------------------------------+----------------------------+----------------------------+---------+-----------+-----------
 b6e90e54-2b35-11eb-bf14-d36eb2a73a4d | b6e787b2-2b35-11eb-bf14-b3c9ba98988e |            1 | AccountOpened  |       1 | b6e87213-2b35-11eb-bf14-03a006a0f3f3 | {"accountId": "b6e787b2-2b35-11eb-bf14-b3c9ba98988e"}                |          |         |                            2 |                          1 | 2020-11-20 14:38:46.907717 |         |           | t
 b70a51f5-2b35-11eb-bf14-d36eb2a73a4d | b6e787b2-2b35-11eb-bf14-b3c9ba98988e |            2 | MoneyDeposited |       1 | b6e87213-2b35-11eb-bf14-03a006a0f3f3 | {"amount": 100, "accountId": "b6e787b2-2b35-11eb-bf14-b3c9ba98988e"} |          |         |                            2 |                          2 | 2020-11-20 14:38:46.91322  |         |           | t
 b72bbca7-2b35-11eb-bf14-d36eb2a73a4d | b6e787b2-2b35-11eb-bf14-b3c9ba98988e |            3 | MoneyWithdrawn |       1 | b72bbca6-2b35-11eb-bf14-03a006a0f3f3 | {"amount": 50, "accountId": "b6e787b2-2b35-11eb-bf14-b3c9ba98988e"}  |          |         |                            1 |                          1 | 2020-11-20 14:38:47.134795 |         |           | t
(3 rows)
```

Events below are published to kafka's bank topic :

Offset 0
```json
{
   "id": "b6e90e54-2b35-11eb-bf14-d36eb2a73a4d",
   "sequenceNum": 1,
   "eventType": "AccountOpened",
   "emissionDate": "2020-11-20T14:38:46.907717",
   "transactionId": "b6e87213-2b35-11eb-bf14-03a006a0f3f3",
   "metadata": null,
   "event": {
      "accountId": "b6e787b2-2b35-11eb-bf14-b3c9ba98988e"
   },
   "context": null,
   "version": 1,
   "published": null,
   "totalMessageInTransaction": 2,
   "numMessageInTransaction": 1,
   "entityId": "b6e787b2-2b35-11eb-bf14-b3c9ba98988e",
   "userId": null,
   "systemId": null
}
```

Offset 1
```json
{
   "id": "b70a51f5-2b35-11eb-bf14-d36eb2a73a4d",
   "sequenceNum": 2,
   "eventType": "MoneyDeposited",
   "emissionDate": "2020-11-20T14:38:46.91322",
   "transactionId": "b6e87213-2b35-11eb-bf14-03a006a0f3f3",
   "metadata": null,
   "event": {
      "accountId": "b6e787b2-2b35-11eb-bf14-b3c9ba98988e",
      "amount": 100
   },
   "context": null,
   "version": 1,
   "published": null,
   "totalMessageInTransaction": 2,
   "numMessageInTransaction": 2,
   "entityId": "b6e787b2-2b35-11eb-bf14-b3c9ba98988e",
   "userId": null,
   "systemId": null
}
```

Offset 2

```json
{
   "id": "b72bbca7-2b35-11eb-bf14-d36eb2a73a4d",
   "sequenceNum": 3,
   "eventType": "MoneyWithdrawn",
   "emissionDate": "2020-11-20T14:38:47.134795",
   "transactionId": "b72bbca6-2b35-11eb-bf14-03a006a0f3f3",
   "metadata": null,
   "event": {
      "accountId": "b6e787b2-2b35-11eb-bf14-b3c9ba98988e",
      "amount": 50
   },
   "context": null,
   "version": 1,
   "published": null,
   "totalMessageInTransaction": 1,
   "numMessageInTransaction": 1,
   "entityId": "b6e787b2-2b35-11eb-bf14-b3c9ba98988e",
   "userId": null,
   "systemId": null
}
```

As we can see, BankEvents aren't published directly into kafka topic, they are wrapped in an "envelop" that contains metadata of the event:

* `id`: unique id of the event
* `sequenceNum`: sequenceNum of the event, sequence is shared between all events therefore sequence num of events for a given id could be non sequential
* `eventType`: the type of the event (`MoneyWithdrawn`, `AccountCreated`, ...)
* `emissionDate`: emissionDate of the event
* `transactionId`: id that can be used to group events emitted by processing a single commands
* `metadata`: json field that can be used to embed additional metadata if needed
* `event`: BankEvent serialized to json
* `context`: json field that can be used to embed additional context information if needed
* `version`: version of the event
* `published`: whether event is published, this field is always null in envelops published in Kafka, but is informed in database
* `totalMessageInTransaction`: total number of messages emitted for the processed command
* `numMessageInTransaction`: index of this message for current transaction
* `entityId`: state (account) identifier
* `userId`: can be use to identify user that emitted command
* `systemId`: can be use to identify system that emitted events

[Complete executable example.](https://github.com/MAIF/thoth/tree/master/demo/demo-postgres-kafka-reactive)


