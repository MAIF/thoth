# Words on API

Thoth is non-blocking by default and rely on vavr but, you can choose if you use it or not.

## Fifty shades of APIs

### Basic API

The basic API use vavr for several reasons :

* `Either` : to handle success or error
* `Option` : instead of java Optional to handle missing values
* `Tuple` : to collect data
* `Tuple0` : to return values not handled (like Void but not exactly the same)
* `List` : instead of java List, because the API is better
* `Lazy` : for lazy values

The basic API, is exposed with package `fr.maif.eventsourcing`.

With the basic API, you'll have to implement something like :

```java
public class BankCommandHandler implements CommandHandler<String, Account, BankCommand, BankEvent, List<String>, TxCtx> {

    @Override
    public CompletionStage<Either<String, Events<BankEvent, List<String>>>> handleCommand(
            TxCtx transactionContext,
            Option<Account> previousState,
            BankCommand command) {
        return switch (command) {
            case Withdraw withdraw -> this.handleWithdraw(previousState, withdraw);
            case Deposit deposit -> this.handleDeposit(previousState, deposit);
            case OpenAccount openAccount -> this.handleOpening(openAccount);
            case CloseAccount close -> this.handleClosing(previousState, close);
        };
    }
}
```

### Vanilla API

If you don't want to use vavr, there is a vanilla API :

* `Either` -> `fr.maif.eventsourcing.Result`: a custom type to handle success or error
* `Option` -> `Optional`
* `Tuple` : internal
* `Tuple0` -> `fr.maif.eventsourcing.Unit` : a custom type to return values not handled (like Void but not exactly the
  same)
* `List` : -> `java.util.List`
* `Lazy` : for lazy values

The package to use is `fr.maif.eventsourcing.vanilla` :

* `AggregateStore`
* `CommandHandler`
* `EventHandler`
* `EventProcessor`
* `EventPublisher`
* `Events`
* `EventStore`
* `ProcessingSuccess`
* `Projection`
* `SimpleCommand`

With the vanilla API, you'll have to implement something like :

```java
public class BankCommandHandler implements CommandHandler<String, Account, BankCommand, BankEvent, List<String>, TxCtx> {

    @Override
    public CompletionStage<Result<String, Events<BankEvent, List<String>>>> handleCommand(
            TxCtx transactionContext,
            Optional<Account> previousState,
            BankCommand command) {
        return switch (command) {
            case Withdraw withdraw -> this.handleWithdraw(previousState, withdraw);
            case Deposit deposit -> this.handleDeposit(previousState, deposit);
            case OpenAccount openAccount -> this.handleOpening(openAccount);
            case CloseAccount close -> this.handleClosing(previousState, close);
        };
    }
}
```

### Blocking API

If you don't to handle the non-blocking aspect, you can implement blocking interface, ie the blocking `CommandHandler`.

The component to use

* `fr.maif.eventsourcing.blocking.CommandHandler` : with standard API (vavr)
* `fr.maif.eventsourcing.vanilla.blocking.CommandHandler` : with vanilla API

With the blocking API, you'll have to implement something like :

```java
public class BankCommandHandler implements CommandHandler<String, Account, BankCommand, BankEvent, List<String>, TxCtx> {

    @Override
    public Result<String, Events<BankEvent, List<String>>> handleCommand(
            TxCtx transactionContext,
            Optional<Account> previousState,
            BankCommand command) {
        return switch (command) {
            case Withdraw withdraw -> this.handleWithdraw(previousState, withdraw);
            case Deposit deposit -> this.handleDeposit(previousState, deposit);
            case OpenAccount openAccount -> this.handleOpening(openAccount);
            case CloseAccount close -> this.handleClosing(previousState, close);
        };
    }
}
```

### Reactive API using reactor

There is a reactor API to use `Mono` instead of `CompletionStage`. The classes are prefixed with `Reactor` :

* `ReactorAggregateStore`
* `ReactorCommandHandler`
* `ReactorEventProcessor`
* `ReactorEventStore`
* `ReactorPostgresKafkaEventProcessorBuilder`
* `ReactorProjection`
* `ReactorTransactionManager`

At the moment, the reactor API use vavr, there is no vanilla API.

With the reactive API, you'll have to implement something like :

```java
public class BankCommandHandler implements CommandHandler<String, Account, BankCommand, BankEvent, List<String>, TxCtx> {

    @Override
    public Mono<Either<String, Events<BankEvent, List<String>>>> handleCommand(
            TxCtx transactionContext,
            Option<Account> previousState,
            BankCommand command) {
        return switch (command) {
            case Withdraw withdraw -> this.handleWithdraw(previousState, withdraw);
            case Deposit deposit -> this.handleDeposit(previousState, deposit);
            case OpenAccount openAccount -> this.handleOpening(openAccount);
            case CloseAccount close -> this.handleClosing(previousState, close);
        };
    }
}
```

## Cheat sheet

### Concepts and vocabulary

* a command : an action coming from the user that the system has to handle
* an event : something that append in the system
* a journal : a place where the events are stored
* a state or aggregate : the current state, the results of the previous events
* a projection : an alternative view of the events, this is a read model

### Overview of interface to implement

Here the list of interface you need or, you could implement (and the reason why).

| Interface                       | Required                     | Role                                                                      |
|---------------------------------|------------------------------|---------------------------------------------------------------------------|
| `State`                         | yes                          | The current state built from events                                       |
| `Command`                       | yes                          | The command represents the data that comes in                             |
| `CommandHandler`                | yes                          | The component that handle commands and produce events                     |
| `EventHandler`                  | yes                          | The component that handle events and produce a state                      |
| `Projection`                    | if you need read models      | The component that handle events and produce read model                   |
| `AbstractDefaultAggregateStore` | if you need snapshots        | The component that load state from events in the journal                  |
| `EventFormat`                   | no                           | The component that serialize / deserialize events                         |
| `JacksonEventFormat`            | yes (prefered)               | The component that serialize / deserialize events to json                 |
| `SimpleFormat`                  | yes                          | The component that serialize / deserialize events ignoring errors         |
| `JacksonSimpleFormat`           | yes                          | The component that serialize / deserialize events to json ignoring errors |
| `EventPublisher`                | no (only if don't use Kafka) | The component that publish events                                         |
| `EventStore`                    | no (only if don't use PG)    | The component that store and query events                                 |
| `EventProcessor`                | no                           | The component orchestrate all the magic                                   |
