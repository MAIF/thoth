# Eventually consistent projections

Eventually consistent projections are built by consuming kafka events.

It's possible using any Kafka client, however Thoth provides an helper for such use cases.

You can use it adding one of these dependencies depending on your stack.  

@@dependency[sbt,Maven,Gradle] {
    symbol="ThothVersion"
    value="$project.version.short$"
    group="fr.maif" artifact="thoth-core-akka" version="ThothVersion"
    group2="fr.maif" artifact2="thoth-core-reactor" version2="ThothVersion"
}

Here is an example using the reactor one : 

```java
EventuallyConsistentProjection.simpleHandler(
    "MeanWithdrawProjection",
    Config.create(topic, groupId, bootstrapServers()),
    eventFormat,
    event -> {
        if(event.value().event instanceof BankEvent.MoneyWithdrawn withdraw) {
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
        return CompletionStages.empty();
    }
).start();
```