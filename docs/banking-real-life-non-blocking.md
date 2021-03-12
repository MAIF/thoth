# Postgres Kafka, non blocking event sourcing

This example is based on [bank example](), we'll replace our InMemoryEventStore by a real Event store using Postgres and Kafka.

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

```xml
<dependency>
    <groupId>fr.maif</groupId>
    <artifactId>thoth-jooq-async_2.13</artifactId>
    <version>...</version>
</dependency>
```

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
public interface BankEvent extends Event {

    Type<MoneyWithdrawn> MoneyWithdrawnV1 = Type.create(MoneyWithdrawn.class, 1L);
    Type<AccountOpened> AccountOpenedV1 = Type.create(AccountOpened.class, 1L);
    Type<MoneyDeposited> MoneyDepositedV1 = Type.create(MoneyDeposited.class, 1L);
    Type<AccountClosed> AccountClosedV1 = Type.create(AccountClosed.class, 1L);

    static Pattern0<MoneyWithdrawn> $MoneyWithdrawn() {
        return Pattern0.of(MoneyWithdrawn.class);
    }
    static Pattern0<AccountOpened> $AccountOpened() {
        return Pattern0.of(AccountOpened.class);
    }
    static Pattern0<MoneyDeposited> $MoneyDeposited() {
        return Pattern0.of(MoneyDeposited.class);
    }
    static Pattern0<AccountClosed> $AccountClosed() {
        return Pattern0.of(AccountClosed.class);
    }

    JsonFormat<BankEvent> format = JsonFormat.of(
            JsonRead.oneOf(_string("type"),
                    caseOf("MoneyWithdrawn"::equals, MoneyWithdrawn.format),
                    caseOf("AccountOpened"::equals, AccountOpened.format),
                    caseOf("MoneyDeposited"::equals, MoneyDeposited.format),
                    caseOf("AccountClosed"::equals, AccountClosed.format)
            ),
            (BankEvent event) -> Match(event).of(
                    Case($MoneyWithdrawn(), MoneyWithdrawn.format::write),
                    Case($AccountOpened(), AccountOpened.format::write),
                    Case($MoneyDeposited(), MoneyDeposited.format::write),
                    Case($AccountClosed(), AccountClosed.format::write)
            )
    );

    @Builder
    @Value
    class MoneyWithdrawn implements BankEvent {

        public final String accountId;
        public final BigDecimal amount;

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

    @Builder
    @Value
    class AccountOpened implements BankEvent {
        public final String accountId;

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

    @Builder
    @Value
    class MoneyDeposited implements BankEvent {
        public final String accountId;
        public final BigDecimal amount;

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

    @Builder
    @Value
    class AccountClosed implements BankEvent {
        public final String accountId;

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
        // Jooq config 
        DefaultConfiguration jooqConfig = new DefaultConfiguration();
        jooqConfig.setSQLDialect(SQLDialect.POSTGRES);
        
        // Vertx config 
        PgConnectOptions options = new PgConnectOptions()
                .setPort(5432)
                .setHost("localhost")
                .setDatabase("eventsourcing")
                .setUser("eventsourcing")
                .setPassword("eventsourcing");
        PoolOptions poolOptions = new PoolOptions().setMaxSize(50);
        PgPool pgPool = PgPool.pool(vertx, options, poolOptions);

        // Glue between jooq and vertx  
        return new ReactivePgAsyncPool(pgPool, jooqConfig);
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

    private ProducerSettings<String, EventEnvelope<BankEvent, Tuple0, Tuple0>> producerSettings(KafkaSettings kafkaSettings) {
        return kafkaSettings.producerSettings(actorSystem, JsonFormatSerDer.of(BankEventFormat.bankEventFormat));        
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
    public Bank(ActorSystem actorSystem,
                BankCommandHandler commandHandler,
                BankEventHandler eventHandler) throws SQLException {
        this.actorSystem = actorSystem;
        this.vertx = Vertx.vertx();
        this.pgAsyncPool = pgAsyncPool(vertx);
        
        var eventStore = eventStore(actorSystem, pgAsyncPool);
        var transactionManager = new ReactiveTransactionManager(pgAsyncPool);
        var producerSettings = producerSettings(settings());
        
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
                .withNoProjections()
                .build();
    }
    //...
}
```

## Usage

Usage remains the same as in [in memory example](./banking.md).

A [docker-compose.yml](../docker-compose.yml) file is available to set-up dev environment.

It exposes a PostgreSQL server on http://localhost:5432/ and a kafdrop instance on http://localhost:9000/.

```java
ActorSystem actorSystem = ActorSystem.create();
BankCommandHandler commandHandler = new BankCommandHandler();
BankEventHandler eventHandler = new BankEventHandler();
Bank bank = new Bank(actorSystem, commandHandler, eventHandler);

String id = bank.createAccount(BigDecimal.valueOf(100)).get().get().currentState.get().id;

bank.withdraw(id, BigDecimal.valueOf(50)).get().get().currentState.get();
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

[Complete executable example.](../demo/demo-postgres-kafka-reactive)

## Next step

[Implement projections to read data differently (non blocking)](./projections-non-blocking.md)
