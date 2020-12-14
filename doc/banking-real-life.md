# Postgres Kafka event sourcing

This example is based on [bank example](), we'll replace our InMemoryEventStore by a real Event store using Postgres and Kafka.

First we need to import `thoth-jooq` module. This module contains an implementation of thoth for Postgres using Jooq.

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

First of all let's swap `thoth-core` dependency with `thoth-jooq`. This new dependency provides everything we need to set up postgres / kafka connection.

```xml
<dependency>
    <groupId>fr.maif</groupId>
    <artifactId>thoth-jooq</artifactId>
    <version>...</version>
</dependency>
```

## Event serialization

Let's start with event reading and writing. We need to declare a serializer to read / write events to DB.

```java
public class BankEventFormat implements JacksonEventFormat<String, BankEvent> {
    @Override
    public Either<String, BankEvent> read(String type, Long version, JsonNode json) {
        return API.Match(Tuple.of(type, version)).option(
                Case(BankEvent.MoneyDepositedV1.pattern2(), () -> Json.fromJson(json, BankEvent.MoneyDeposited.class)),
                Case(BankEvent.MoneyWithdrawnV1.pattern2(), () -> Json.fromJson(json, BankEvent.MoneyWithdrawn.class)),
                Case(BankEvent.AccountClosedV1.pattern2(), () -> Json.fromJson(json, BankEvent.AccountClosed.class)),
                Case(BankEvent.AccountOpenedV1.pattern2(), () -> Json.fromJson(json, BankEvent.AccountOpened.class))
        )
                .toEither(() -> "Unknown event type " + type + "(v" + version + ")")
                .flatMap(jsResult -> jsResult.toEither().mapLeft(errs -> errs.mkString(",")));
    }

    @Override
    public JsonNode write(BankEvent event) {
        return Json.toJson(event, JsonWrite.auto());
    }
}
```

We implemented this using [functionnal-json](https://github.com/MAIF/functional-json) library, since it provides nice utilities to handle / aggregate deserialization errors.

To allow event serialization / deserialization we also need to add some Jackson annotations (`@JsonCreator` and `@JsonProperty`) to events' constructors.

```java
public abstract class BankEvent implements Event {
    public static Type<MoneyWithdrawn> MoneyWithdrawnV1 = Type.create(MoneyWithdrawn.class, 1L);
    public static Type<AccountOpened> AccountOpenedV1 = Type.create(AccountOpened.class, 1L);
    public static Type<MoneyDeposited> MoneyDepositedV1 = Type.create(MoneyDeposited.class, 1L);
    public static Type<AccountClosed> AccountClosedV1 = Type.create(AccountClosed.class, 1L);

    public final String accountId;

    public BankEvent(String accountId) {
        this.accountId = accountId;
    }

    @Override
    public String entityId() {
        return accountId;
    }

    public static class MoneyWithdrawn extends BankEvent {
        public final BigDecimal amount;
        @JsonCreator
        public MoneyWithdrawn(@JsonProperty("accountId")String account, @JsonProperty("amount")BigDecimal amount) {
            super(account);
            this.amount = amount;
        }

        @Override
        public Type<?> type() {
            return MoneyWithdrawnV1;
        }
    }

    public static class AccountOpened extends BankEvent {
        @JsonCreator
        public AccountOpened(@JsonProperty("accountId")String id) {
            super(id);
        }

        @Override
        public Type<?> type() {
            return AccountOpenedV1;
        }
    }

    public static class MoneyDeposited extends BankEvent {
        public final BigDecimal amount;
        @JsonCreator
        public MoneyDeposited(@JsonProperty("accountId")String id, @JsonProperty("amount")BigDecimal amount) {
            super(id);
            this.amount = amount;
        }

        @Override
        public Type<?> type() {
            return MoneyDepositedV1;
        }
    }

    public static class AccountClosed extends BankEvent {
        @JsonCreator
        public AccountClosed(@JsonProperty("accountId")String id) {
            super(id);
        }

        @Override
        public Type<?> type() {
            return AccountClosedV1;
        }
    }
}
```

## Database connection

Speaking of database, we also need to set up a database connection somewhere.

In the sample application, this is made in `Bank` class, in real world application, this could be made in some configuration class.

```Java
public class Bank {
    // ...
    private DataSource dataSource() {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setServerName("localhost");
        dataSource.setPassword("eventsourcing");
        dataSource.setUser("eventsourcing");
        dataSource.setDatabaseName("eventsourcing");
        dataSource.setPortNumbers(5432);
        return dataSource;
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
public class BankCommandHandler implements CommandHandler<String, Account, BankCommand, BankEvent, Tuple0, Connection> {
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

    private ProducerSettings<String, EventEnvelope<BankEvent, Tuple0, Tuple0>> producerSettings(
            KafkaSettings kafkaSettings,
            JacksonEventFormat<String, BankEvent> eventFormat) {
        return kafkaSettings.producerSettings(actorSystem, JsonSerializer.of(
                eventFormat,
                JacksonSimpleFormat.empty(),
                JacksonSimpleFormat.empty()
            )
        );
    }
    //...
}
```


## Kafka event publisher

Using above methods, we need to define a `KafkaEventPublisher`, that will handle publication of events on kafka once they'll be store in the database.

```java
public class Bank {
    //...
    private KafkaEventPublisher<BankEvent, Tuple0, Tuple0> kafkaEventPublisher(
            ActorSystem actorSystem,
            ProducerSettings<String, EventEnvelope<BankEvent, Tuple0, Tuple0>> producerSettings,
            String topic) {
        return new KafkaEventPublisher<>(actorSystem, producerSettings, topic);
    }
    //...
}
```


## Event store

We need to change our `EventStore` implementation to `PostgresEventStore`.
This new event store will store event in Postgres instead of in memory.

```java
public class Bank {
    //...
    private PostgresEventStore<BankEvent, Tuple0, Tuple0> eventStore(
            ActorSystem actorSystem,
            KafkaEventPublisher<BankEvent, Tuple0, Tuple0> kafkaEventPublisher,
            DataSource dataSource,
            ExecutorService executorService,
            TableNames tableNames,
            JacksonEventFormat<String, BankEvent> jacksonEventFormat) {
        return PostgresEventStore.create(actorSystem, kafkaEventPublisher, dataSource, executorService, tableNames, jacksonEventFormat);
    }
    //...
}
```

## Event processor

The last step is to swap our `EventProcessor` with `PostgresKafkaEventProcessor`.

To instantiate this new EventProcessor, we'll need everything we defined previously, and additional instances:
* 


```java
public class Bank {
    //...
    public Bank(ActorSystem actorSystem,
                    BankCommandHandler commandHandler,
                    BankEventHandler eventHandler
                    ) throws SQLException {
        String topic = "bank";
        this.actorSystem = actorSystem;
        JacksonEventFormat<String, BankEvent> eventFormat = new BankEventFormat();
        ProducerSettings<String, EventEnvelope<BankEvent, Tuple0, Tuple0>> producerSettings = producerSettings(settings(), eventFormat);
        DataSource dataSource = dataSource();
        TableNames tableNames = tableNames();

        ExecutorService executorService = Executors.newFixedThreadPool(5);
        JdbcTransactionManager transactionManager = new JdbcTransactionManager(dataSource(), executorService);

        KafkaEventPublisher<BankEvent, Tuple0, Tuple0> kafkaEventPublisher = kafkaEventPublisher(actorSystem, producerSettings, topic);

        PostgresEventStore<BankEvent, Tuple0, Tuple0> eventStore = eventStore(
                actorSystem,
                kafkaEventPublisher,
                dataSource,
                executorService,
                tableNames,
                eventFormat
        );

        this.eventProcessor = new PostgresKafkaEventProcessor<>(new PostgresKafkaEventProcessor.PostgresKafkaEventProcessorConfig<>(
                actorSystem,
                eventStore,
                transactionManager,
                commandHandler,
                eventHandler,
                List.of(meanWithdrawProjection),
                kafkaEventPublisher
        ));
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

[Complete executable example.](../demo/demo-postgres-kafka)

## Next step

* [Implement projections to read data differently](./projections.md)