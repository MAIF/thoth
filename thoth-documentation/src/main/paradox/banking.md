# In memory example

This sample explains how to implement event sourcing for a simple use case of bank account.

In this example, we will focus on managing accounts one by one.

We will see later @ref:[how to manager multiple accounts at once](advanced/multi-command.md).

Here is the process modeling of what happens: 

![](img/thoth_bank_account.jpg) 

## Model (State) 

Let's start with a simple bank account representation.

```java
public class Account extends AbstractState {
    public String id;
    public BigDecimal balance;
}
```

This Account class needs to extend `AbstractState`.
It represents the state of one Account at a given time.

## Command

Here are some commands that our system should accept

* withdraw
* deposit
* close
* open

Let's start small with "open" commands :

```java
public sealed interface BankCommand extends SimpleCommand {

    record OpenAccount(Lazy<String> id, BigDecimal initialBalance) implements BankCommand {
        @Override
        public Lazy<String> entityId() {
            return id;
        }

        @Override
        public Boolean hasId() {
            return false;
        }
    }
}
```

There's a lot going on here:

* Commands are a [sum type](https://en.wikipedia.org/wiki/Tagged_union), therefore it can be implemented using a Java interface

* Our commands need to implement `SimpleCommand`, there is a more complete version of this class called `Command` that allows providing of additional information such as metadata. More on that later.

* Our class needs to implement an `entityId` method that should return something that identifies uniquely our account.
This method returns a [Vavr Lazy](https://docs.vavr.io/#_lazy) object, which is useful when id isn't known yet.

* We used the `sealed` syntax from the last jdk versions. With this you can have an exhaustive list of class that implements the interface. 

* `OpenAccount` implementation is slightly different from others : we take a `Lazy<String>` instead of a String for id field, and overload `hasId` method.
In our system, account id will be generated as random UUID, and we don't want to generate an id while we didn't check the correctness of the `OpenAccount` command.
That's why we use a `Lazy` for the id : to defer id generation.
The overload of `hasId` method to make it return `false` indicates that our command does not yet have an id.

## Event

Our system will generate an event when receiving our withdraw command (if correct).

One possible naming convention for events is using passive way, so let's call our events `AccountOpened`, `MoneyWithdrawn` and `MoneyDeposited`.

```java
public sealed interface BankEvent extends Event {
  Type<AccountOpened> AccountOpenedV1 = Type.create(AccountOpened.class, 1L);
  Type<MoneyDeposited> MoneyDepositedV1 = Type.create(MoneyDeposited.class, 1L);

  String accountId();

  default String entityId() {
    return accountId();
  }

  record AccountOpened(String accountId) implements BankEvent {
    @Override
    public Type<AccountOpened> type() {
      return AccountOpenedV1;
    }
  }

  record MoneyDeposited(String accountId, BigDecimal amount) implements BankEvent {
    @Override
    public Type<MoneyDeposited> type() {
      return MoneyDepositedV1;
    }
  }
}

```

Let's decompose this snippet:

* Like commands, events are a [sum type](https://en.wikipedia.org/wiki/Tagged_union), however we used an abstract class instead of an interface to factorize `entityId` logic
* Event must implement two methods
  * entityId that must identify uniquely an account
  * a type, that can be used to perform [Vavr pattern matching](https://docs.vavr.io/#_the_basics_of_match_for_java), in addition to the name of the event, the type store its version, facilitating version bump of events.

## From command to event

Now that we got a state representation, some commands and events, it's time to implement our first command handler.

Let's start small with account creation:

```java
import java.util.concurrent.CompletableFuture;

public class BankCommandHandler implements CommandHandler<String, Account, BankCommand, BankEvent, Tuple0, Tuple0> {
  @Override
  public CompletableFuture<Either<String, Events<BankEvent, Tuple0>>> handleCommand(
          Tuple0 transactionContext,
          Option<Account> previousState,
          BankCommand command) {
    return CompletableFuture.supplyAsync(() -> switch (command) {
      case OpenAccount openAccount -> this.handleOpening(openAccount);
    });
  }

  private Either<String, Events<BankEvent, Tuple0>> handleOpening(
          BankCommand.OpenAccount opening) {
    if (opening.initialBalance.compareTo(BigDecimal.ZERO) < 0) {
      return Left("Initial balance can't be negative");
    }

    String newId = opening.id.get();
    List<BankEvent> events = List(new BankEvent.AccountOpened(newId));
    if (opening.initialBalance.compareTo(BigDecimal.ZERO) > 0) {
      events = events.push(new BankEvent.MoneyDeposited(newId, opening.initialBalance));
    }

    return Right(Events.events(events));
  }
}
```

This implementation may look cumbersome, so let's decompose it again:

* when implementing `CommandHanler`, we need to provide 6 parameters:
  * first one is the error format, if commandHandler is given an invalid event, it should return an error of this type,
  here we chose to use the good old `String` type however a more complex error type should be used in real life scenario
  * second one is the class representing the state manipulated by our application : `Account`
  * third one is the class representing commands: `BankCommand`
  * fourth one is the class representing events: `BankEvent`
  * fifth one can be used to represent some additional information (such as warnings) resulting from command processing,
    as we intend to keep this example as simple as possible, it is not used here
  * sixth one can be used to provide a transaction context that can be used to validate command (for instance a JDBC connection, or a Cassandra session)
    we don't need this yet
* implementations of `CommandHandler` must implement `handleCommand` method
  * this method returns a `CompletionStage` because some times we need to perform some I/O operation to validate commands (e.g. make an HTTP call, or read something in a database)
  * this `CompletionStage` wraps an `Either` that can contain an error (if command processing failed) or an instance of `Events` class,
  which is just a package containing a list of `Event` generated from the command and additional information if needed.
  * this method provides 3 arguments: 
    * a transaction context (not used in this example)
    * an Option representing the previous state of the account, it can be empty if there is no previous state (i.e. if account does not exist)
    * the command to process
* we used the new java switch to pattern match the command, the compiler will raise an error if the exhaustiveness of cases is not handled.  
* our implementation of account creation checks that initial balance is positive, and then retrieve id of the new account (random UUID is generated at this moment).
In this case we don't have to bother with previous state : since our command indicates that it has no id, there is no previous state to retrieve.
* handling of account creation command can generate one or two events : when initial balance is positive, an event of deposit is generated in addition to the creation event

## State update

The last step is to update the state of our account using our `AccountOpened` event.

```java
public class BankEventHandler implements EventHandler<Account, BankEvent> {
    @Override
    public Option<Account> applyEvent(
            Option<Account> previousState,
            BankEvent event) {
        return switch(event) {
            case AccountOpened accountOpened -> BankEventHandler.handleAccountOpened(accountOpened);
            case MoneyDeposited deposit -> BankEventHandler.handleMoneyDeposited(previousState, deposit);
        };
    }

    private static Option<Account> handleAccountOpened(BankEvent.AccountOpened event) {
        Account account = new Account();
        account.id = event.accountId;
        account.balance = BigDecimal.ZERO;

        return Option.some(account);
    }

    private static Option<Account> handleMoneyDeposited(
            Option<Account> previousState,
            BankEvent.MoneyDeposited event) {
        return previousState.map(state -> {
            state.balance = state.balance.add(event.amount);
            return state;
        });
    }
}
```

Our `BankEventHandler` implements `EventHandler`, which takes two parameters : state representation (`Account`), and events (`BankEvent`).
The `applyEvent` method gives us two parameters:
* an `Option` representing previous state, it's empty if there is no previous state for the event's entityId
* the `Event` to apply to the previous state (if any), to get the next state

Once again we used pattern matching to get event type.
As for commands, we defined a method for each event type.
Since computing next state for an existing state and an event is a pure function, it's a good practice to make these methods static.  

This method returns an `Option`, that should be empty if the `Account` is to be closed : future call implying this account will have an empty previousState.

## Wiring all the things

Now that we defined every step from command to state update, it's time to wire-up everything:

```java
public class Bank {
    private final EventProcessor<String, Account, BankCommand, BankEvent, Tuple0, Tuple0, Tuple0, Tuple0> eventProcessor;
    private static final TimeBasedGenerator UUIDgenerator = Generators.timeBasedGenerator();


    public Bank(ActorSystem actorSystem,
                BankCommandHandler commandHandler,
                BankEventHandler eventHandler
                ) {
        InMemoryEventStore<BankEvent, Tuple0, Tuple0> eventStore = InMemoryEventStore.create(actorSystem);
        TransactionManager<Tuple0> transactionManager = noOpTransactionManager();
        this.eventProcessor = new EventProcessorImpl<>(
                eventStore,
                transactionManager,
                new DefaultAggregateStore<>(eventStore, eventHandler, actorSystem, transactionManager),
                commandHandler,
                eventHandler,
                List.empty()
        );
    }

    private TransactionManager<Tuple0> noOpTransactionManager() {
        return new TransactionManager<>() {
            @Override
            public <T> CompletionStage<T> withTransaction(Function<Tuple0, CompletionStage<T>> function) {
                return function.apply(Tuple.empty());
            }
        };
    }

    public CompletionStage<Either<String, ProcessingSuccess<Account, BankEvent, Tuple0, Tuple0, Tuple0>>> createAccount(BigDecimal amount) {
        Lazy<String> lazyId = Lazy.of(() -> UUIDgenerator.generate().toString());
        return eventProcessor.processCommand(new BankCommand.OpenAccount(lazyId, amount));
    }

    public CompletionStage<Option<Account>> findAccountById(String id) {
        return eventProcessor.getAggregate(id);
    }
}
```

This `Bank` class is the one the rest of our application should use.

It instantiates an `EventProcessor` that takes 8 parameters:
* Error representation: `String` as usual
* State representation: `Account`
* Command representation: `BankCommand`
* Event representation: `BankEvent`
* TransactionContext, Message, Metadata and Context : all `Tuple0` since they are not used in this example

This EventProcessor takes our EventHandler and CommandHandler.
This class is the one that really wires everything up.

When we call `processCommand` method, an EventProcessor:
1. give it to its `CommandHanler` (here `BankCommandHandler`) along with the previous state (if any) to get events
2. store events in an `EventStore`: in this example an `InMemoryEventStore`, in a real use case it would be a database based event store (like `PostgresEventStore`)
3. update projection with events (more on that later)
4. publish events to Kafka: this is done by the `EventStore`, but since we used an `InMemoryEventStore`it's not done in this example
5. returns a `CompletionStage<Either<String, ProcessingSuccess<...>>>` :
  * a `CompletionStage` since all above operations usually includes I/O
  * an `Either` to indicate that result could be an error (e.g. if command is incorrect)
  * a `ProcessingError` that contains various information about the process : current (new) state, previous state, events, ...

When we call `getAggregate`, an EventProcessor:
1. load all events for the given entityId
2. sequentially apply all events to an empty state
3. return the final state as an `Option` (it may be empty, for instance if the account is closed)


## Usage

```java
BankCommandHandler commandHandler = new BankCommandHandler();
BankEventHandler eventHandler = new BankEventHandler();
Bank bank = new Bank(actorSystem, commandHandler, eventHandler);

bank.createAccount(BigDecimal.valueOf(100))
        .whenComplete((either, e) -> {
          if (Objects.nonNull(e)) {
            either.map(result -> result.currentState
                .peek(account -> System.out.println(account.balance))
            )
            .peekLeft(System.err::println);
          } else {
            e.printStackTrace();
          }
        });
```

## Complete example

See [complete example](https://github.com/MAIF/thoth/tree/master/demo/demo-in-memory) of some other commands (withdraw, deposit, close, ...).
