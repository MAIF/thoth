# Custom message ordering

Kafka does not guarantee event ordering for a topic.

This can lead to troubles in case where a business process is composed of several events.

Let's take an example with our banking application, with a basic scenario.

1. open an account
2. withdraw money
3. close the account

In this example, it's critical for consumers to consume events in the correct order,
otherwise there is a risk of inconsistent state.

A solution is to send all these events on the same Kafka partition, since Kafka guarantees order for a partition.

To achieve this, we need to provide kafka a hash along with our event.
Events with the same hash will be published on the same partition.

By default, thoth use entityId of the state as a hash.
In our case, this guarantee that all events regarding an account will be published / consumed in correct order.

In some other cases, we may want to sort events on another criteria, or on a combination of criterion.

Let's say we want to order our BankEvents not only using `accountId`, but also with a `customerId`.

```java
public static class MoneyWithdrawn extends BankEvent {
    public final BigDecimal amount;
    public final String customerId;
    @JsonCreator
    public MoneyWithdrawn(
            @JsonProperty("accountId")String account,
            @JsonProperty("amount")BigDecimal amount,
            @JsonProperty("customerId")String customerId) {
        super(account);
        this.amount = amount;
        this.customerId = customerId;
    }

    @Override
    public Type<MoneyWithdrawn> type() {
        return MoneyWithdrawnV1;
    }

    @Override
    public String hash() {
        return accountId + customerId;
    }
}
```

All we need to do is to override method `hash`. Events with the same hash are guaranteed to go on the same Kafka partition.

## Next step

[Make your read faster with aggregateStore](./aggregatestore.md)