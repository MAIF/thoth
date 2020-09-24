# An event sourcing implementation for async JOOQ

 
Concrete implementation of the event sourcing api with reactive postgresql client. 

## Database setup

First you need to create the journal in postgresql : 

```postgresql
CREATE TABLE IF NOT EXISTS vikings_journal (
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

CREATE INDEX vikings_sequence_num_idx ON vikings_journal (sequence_num);

CREATE INDEX vikings_entity_id_idx ON vikings_journal (entity_id);

CREATE INDEX vikings_user_id_idx ON vikings_journal (user_id);

CREATE INDEX vikings_system_id_idx ON vikings_journal (system_id);

CREATE INDEX vikings_emission_date_idx ON vikings_journal (emission_date);

CREATE INDEX vikings_published_idx ON vikings_journal(published);

CREATE SEQUENCE if not exists vikings_journal_id;

CREATE SEQUENCE if not exists vikings_sequence_num;
```

## Wire together all the components 

Here is an example of what you could do to wire all the components together: 

```java
// Jooq config 
DefaultConfiguration jooqConfig = new DefaultConfiguration();
jooqConfig.setSQLDialect(SQLDialect.POSTGRES_10);

// Vertx client 
PgConnectOptions options = new PgConnectOptions()
        .setPort(5555)
        .setHost("localhost")
        .setDatabase("my-db")
        .setUser("john.doe")
        .setPassword("you'll never find");
PoolOptions poolOptions = new PoolOptions().setMaxSize(50);
Vertx vertx = Vertx.vertx();
PgPool client = PgPool.pool(vertx, options, poolOptions);

PgAsyncPool pgAsyncPool = new ReactivePgAsyncPool(client, jooqConfig);
TransactionManager<PgAsyncTransaction> transactionManager = new ReactiveTransactionManager(pgAsyncPool);

ActorSystem actorSystem = ActorSystem.create();

//You can create the processor like this (this is a lot of stuff !!!):
var vikingEventProcessor =  new ReactivePostgresKafkaEventProcessor<>(
        new ReactivePostgresKafkaEventProcessor.PostgresKafkaEventProcessorConfig<>(    
            actorSystem,
            new TableNames("vikings_journal", "vikings_sequence_num"),
            pgAsyncPool,
            "viking-topic",
            KafkaSettings.producerSettings( // A kafka producer with the serializers for our events:
                    actorSystem,
                    JsonSerializer.of(
                        VikingEventJsonFormatter.vikingJsonFormat(), 
                        JacksonSimpleFormat.empty(), 
                        JacksonSimpleFormat.empty()
                    )
            ),
            transactionManager,
            vikingsCommandHandler(),
            vikingEventHandler(),
            List.of(vikingProjection()),
            VikingEventJsonFormatter.vikingJsonFormat(), 
            JacksonSimpleFormat.empty(), 
            JacksonSimpleFormat.empty(),
            1000
        )
);
```

Or with a manual instanciation of all the components:  

```java
// An implementation of the EventPublisher for kafka
var eventPublisher = new KafkaEventPublisher<VikingEvent, Nothing, Nothing>( 
                actorSystem,
                KafkaSettings.producerSettings( // A kafka producer with the serializers for our events:
                        actorSystem,
                        JsonSerializer.of(
                            VikingEventJsonFormatter.vikingJsonFormat(), 
                            JacksonSimpleFormat.empty(), 
                            JacksonSimpleFormat.empty()
                        )
                ),
                "viking-topic",
                1000 // buffer size for messages to publish 
        )

// The implementation of EventStore for postgresql with reactive client 
var eventStore = new ReactivePostgresEventStore<PgAsyncTransaction, VikingEvent, Nothing, Nothing>(
        actorSystem,
        eventPublisher,
        pgAsyncPool,
        new TableNames("vikings_journal", "vikings_sequence_num"),
        VikingEventJsonFormatter.vikingJsonFormat(),
        JacksonSimpleFormat.empty(),
        JacksonSimpleFormat.empty()
);

// The default aggregate store is ok for most cases
var aggregateStore = new DefaultAggregateStore<Viking, VikingEvent, Nothing, Nothing, PgAsyncTransaction>(
        eventStore,
        vikingEventHandler(),
        actorSystem,
        transactionManager
);

// At the end 
var vikingEventProcessor = new ReactivePostgresKafkaEventProcessor<>(
        new ReactivePostgresKafkaEventProcessor.PostgresKafkaEventProcessorConfig<>(
                eventStore,
                transactionManager,
                aggregateStore,
                vikingsCommandHandler(),
                vikingEventHandler(),
                List.of(vikingProjection()),
                eventPublisher
        )
);
```  

