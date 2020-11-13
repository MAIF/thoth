# Event sourcing 

This module provides helpers to implement event sourcing in your application. 

The concepts

 * a `command` is sent by the client.
 * `events` are read from the database. A `state` is calculated from these `events`
 * the `command` is validated using the current `state`
   * if the `command` is valid then one or more `event` are returned and stored in database    
   * if the `command` is not valid and error is returned to the client 
 * One or more `projections` are calculated from the `events` 
   *  These `projections` are the read model optimised for querying 

To implement event-sourcing you need : 
 
 * a command handler 
   * this component takes a command and must return either an error or a list of events 
 * an event handler 
   * this component takes the current state and an event and must return the next state 
 * serializer / deserializer 
   * the events are stored in the database and published to an event store. This component handle these write and read operations.
 * 0 to n projections
 
 
This library, rely on Vavr for functional programming data classes and pattern matching. 
 
This library provides interfaces that could be implemented with various datastore. 
Event sourcing is async by default, using vavr future for async process and akka stream for reactive streams. 

This library provides two implementations that are production ready : 
* an event publisher based on kafka 
* an event store based on postgresql and jooq 

With these implementations, events and projections are stored in the same transaction. 
The events are published once the transaction is committed. If the publication failed, a process will try to resend the events. 

 
## The viking domain 

In this example, we will handle vikings. The state will be 

```java
public class Viking implements State<Viking> {

        public final String id;
        public final String name;
        public Long sequenceNum;

        public Viking(String id, String name) {
            this.id = id;
            this.name = name;
        }

        @Override
        public Long sequenceNum() {
            return sequenceNum;
        }

        @Override
        public Viking withSequenceNum(Long sequenceNum) {
            this.sequenceNum = sequenceNum;
            return this;
        }
}

```

The commands need to implement `Command`. Commands are a sum type, in java we can do this using an interface :

```java
public interface VikingCommand extends Command<Tuple0, Tuple0> {

    Type<CreateViking> CreateVikingV1 = Type.create(CreateViking.class, 1L);
    Type<UpdateViking> UpdateVikingV1 = Type.create(UpdateViking.class, 1L);
    Type<DeleteViking> DeleteVikingV1 = Type.create(DeleteViking.class, 1L);

    class CreateViking implements VikingCommand {
        public String id;
        public String name;

        public CreateViking(String id, String name) {
            this.id = id;
            this.name = name;
        }

        @Override
        public Lazy<String> entityId() {
            return Lazy.of(() ->id);
        }
    }

    class UpdateViking implements VikingCommand {
        public String id;
        public String name;

        public UpdateViking(String id, String name) {
            this.id = id;
            this.name = name;
        }

        @Override
        public Lazy<String> entityId() {
            return Lazy.of(() ->id);
        }
    }

    class DeleteViking implements VikingCommand {
        public String id;

        public DeleteViking(String id) {
            this.id = id;
        }

        @Override
        public Lazy<String> entityId() {
            return Lazy.of(() ->id);
        }
    }
}
```

The events need to implement `Event`. Events are a sum type, in java we can do this using an interface : 
 
```java
public interface VikingEvent extends Event {

    // Types are needed to handle event versionning : 
    Type<VikingCreated> VikingCreatedV1 = Type.create(VikingCreated.class, 1L);
    Type<VikingUpdated> VikingUpdatedV1 = Type.create(VikingUpdated.class, 1L);
    Type<VikingDeleted> VikingDeletedV1 = Type.create(VikingDeleted.class, 1L);

    class VikingCreated implements VikingEvent {
        public String id;
        public String name;

        public VikingCreated(String id, String name) {
            this.id = id;
            this.name = name;
        }
        
        @Override
        public Type<VikingCreated> type() {
            return VikingCreatedV1;
        }

        @Override
        public String entityId() {
            return id;
        }
    }

    class VikingUpdated implements VikingEvent {
        public String id;
        public String name;

        public VikingUpdated(String id, String name) {
            this.id = id;
            this.name = name;
        }

        @Override
        public Type<VikingUpdated> type() {
            return VikingUpdatedV1;
        }

        @Override
        public String entityId() {
            return id;
        }
    }


    class VikingDeleted implements VikingEvent {
        public String id;

        public VikingDeleted(String id) {
            this.id = id;
        }

        @Override
        public Type<VikingDeleted> type() {
            return VikingDeletedV1;
        }

        @Override
        public String entityId() {
            return id;
        }
    }
}

``` 
 
### Command handler 

The command handler should implement: 

```java
public interface CommandHandler<Error, State, Command, Event, Message, TxCtx> {

    Future<Either<Error, Tuple2<List<Event>, Message>>> handleCommand(TxCtx ctx, Option<State> state, Command command);
}
```

in our viking case we will have something like : 

 * `CommandHandler<String, Viking, VikingCommand, VikingEvent, Tuple0, Tuple0>` :
   * String : the type of error
   * Viking : the state 
   * VikingCommand : the command
   * VikingEvent : the event 
   * The message : something the command handler can return in addition of the events 
   * The transaction context : if needed a context that can be used to validate command. For example a JDBC connection or a Cassandra session.    
  
```java
public class VikingCommandHandler implements CommandHandler<String, Viking, VikingCommand, VikingEvent, Tuple0, Tuple0> {
    @Override
    public Future<Either<String, Events<VikingEvent, Tuple0>>> handleCommand(Tuple0 tuple0, Option<Viking> option, VikingCommand vikingCommand) {
        return Future.successful(
                // Here we pattern match on the type of the command
                Match(vikingCommand).of(
                        Case(VikingCommand.CreateVikingV1.pattern(), e ->
                                // Here we can add validation and reject the command if needed
                                Right(Events.events(new VikingEvent.VikingCreated(e.id, e.name)))
                        ),
                        Case(VikingCommand.UpdateVikingV1.pattern(), e ->
                                Right(Events.events(new VikingEvent.VikingUpdated(e.id, e.name)))
                        ),
                        Case(VikingCommand.DeleteVikingV1.pattern(), e ->
                                Right(Events.events(new VikingEvent.VikingDeleted(e.id)))
                        )
                )
        );
    }
}
```

### Event handler

The event handler should implement: 

```java
public interface EventHandler<State, Event> {

    default Option<State> deriveState(Option<State> state, List<Event> events) {
        return events.foldLeft(state, this::applyEvent);
    }

    Option<State> applyEvent(Option<State> state, Event events);
}
```

in our viking case we will have something like :
* `EventHandler<Viking, VikingEvent>`
  * `Viking` the state 
  * `VikingEvent` the event 

```java
public class VikingEventHandler implements EventHandler<Viking, VikingEvent> {
    @Override
    public Option<Viking> applyEvent(Option<Viking> state, VikingEvent event) {
        return Match(event).of(
                Case(VikingEvent.VikingCreatedV1.pattern(), e -> Option.of(new Viking(e.id, e.name))),
                Case(VikingEvent.VikingUpdatedV1.pattern(), e -> Option.of(new Viking(e.id, e.name))),
                Case(VikingEvent.VikingDeletedV1.pattern(), e -> Option.none())
        );
    }
}
```

This function should apply an `Event` to a `Option<State>`(that can be empty if the state doesn't exist). 
The new state is returned, if the event is a delete event, then an empty option should be returned.  

### Projections

The projection should implement:

```java
public interface Projection<TxCtx, E extends Event, Meta, Context> {
    Future<Tuple0> storeProjection(TxCtx ctx, List<EventEnvelope<E, Meta, Context>> events);
}
```

In our case we implement a projection in memory to perform look up by name :

```java
public class VikingProjection implements Projection<Tuple0, VikingEvent, Tuple0, Tuple0> {

        public ConcurrentHashMap<String, Viking> data = new ConcurrentHashMap<>();

        @Override
        public Future<Tuple0> storeProjection(Tuple0 unit, List<EventEnvelope<VikingEvent, Tuple0, Tuple0>> events) {
            return Future.of(() -> {
                events.forEach(event -> {
                    String entityId = event.entityId;
                    VikingEvent vikingEvent = event.event;
                    Option<Viking> viking = Match(vikingEvent).of(
                            Case(VikingCreatedV1.pattern(), e -> Option.of(new Viking(e.id, e.name))),
                            Case(VikingUpdatedV1.pattern(), e -> Option.of(new Viking(e.id, e.name))),
                            Case(VikingDeletedV1.pattern(), e -> Option.none())
                    );
                    viking.forEach(v -> data.put(entityId, v));
                });
                return unit();
            });
        }

        public Future<Option<Viking>> getById(String id) {
            return Future(Option(data.get(id)));
        }

        public Future<List<Viking>> findByName(String name) {
            return Future(
                    List.ofAll(data.values())
                            .filter(v -> v.name.equals(name))
            );
        }    
}
```


### Wiring all together 

```java
public static class Vikings {

    private final EventProcessor<String, Viking, VikingCommand, VikingEvent, Tuple0, Tuple0, Tuple0, Tuple0> eventProcessor;
    private final VikingProjection vikingReadModel = new VikingProjection();

    public Vikings(ActorSystem actorSystem) {
        InMemoryEventStore<Tuple0, VikingEvent, Tuple0, Tuple0> eventStore = InMemoryEventStore.create(actorSystem);
        VikingEventHandler eventHandler = new VikingEventHandler();
        TransactionManager<Tuple0> transactionManager = new TransactionManager<>() {
            @Override
            public <T> Future<T> withTransaction(Function<Tuple0, Future<T>> function) {
                return function.apply(Tuple.empty());
            }
        };
        this.eventProcessor =  new EventProcessor<>(
                eventStore,
                transactionManager,
                new DefaultAggregateStore<>(eventStore, eventHandler, actorSystem, transactionManager),
                new VikingCommandHandler(),
                eventHandler,
                List.of(vikingReadModel)
        );
    }

    public Future<Either<String, ProcessingSuccess<Viking, VikingEvent, Tuple0, Tuple0, Tuple0>>> create(VikingCommand.CreateViking command) {
        return eventProcessor.processCommand(command);
    }

    public Future<Either<String, ProcessingSuccess<Viking, VikingEvent, Tuple0, Tuple0, Tuple0>>> update(VikingCommand.UpdateViking command) {
        return eventProcessor.processCommand(command);
    }

    public Future<Either<String, ProcessingSuccess<Viking, VikingEvent, Tuple0, Tuple0, Tuple0>>> delete(VikingCommand.DeleteViking command) {
        return eventProcessor.processCommand(command);
    }

    public Future<Option<Viking>> getById(String id) {
        return vikingReadModel.getById(id);
    }

    public Future<List<Viking>> findByName(String name) {
        return vikingReadModel.findByName(name);
    }
}
```