# Eventually consistent projections

Eventually consistent projections are built by consuming kafka events.

It's possible using any Kafka client, however Thoth provides an helper for such use cases.

```java
EventuallyConsistentProjection.create(
    actorSystem,
    "MeanWithdrawProjection",
    EventuallyConsistentProjection.Config.create("bank", "MeanWithdrawProjection", bootstrapServer),
    eventFormat,
    envelope -> {
        if(envelope.event instanceof BankEvent.MoneyWithdrawn withdraw) {
            try(final PreparedStatement statement = dataSource.getConnection().prepareStatement("""
                    insert into withdraw_by_month (client_id, month, year, withdraw, count) values (?, ?, ?, ?, 1)
                        on conflict on constraint WITHDRAW_BY_MONTH_UNIQUE
                        do update set withdraw = withdraw_by_month.withdraw + EXCLUDED.withdraw, count=withdraw_by_month.count + 1
                """)
            ) {
                statement.setString(1, envelope.entityId);
                statement.setString(2, envelope.emissionDate.getMonth().name().toUpperCase());
                statement.setInt(3, envelope.emissionDate.getYear());
                statement.setBigDecimal(4, withdraw.amount);

                statement.execute();
            } catch (SQLException ex) {
                LOGGER.error("Failed to update stats projection", ex);
            }
        }
        return Future.successful(Tuple.empty());
    }
).start();
```