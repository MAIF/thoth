# Database configuration

There is some important index to create on your database to have correct performance on event read / publication.

You should at least have indexes on `entity_id`, `published` and `sequence_num` columns:
* `entity_id` because it's used by CommandHandler to compute previous state
* `published` because it's used to load unpublished events before publishing them in Kafka
* `sequence_num` because it's used to sort events when building previous state

```sql
CREATE INDEX app_sequence_num_idx ON app_journal (sequence_num);
CREATE INDEX app_entity_id_idx ON app_journal (entity_id);
CREATE INDEX app_published_idx ON app_journal(published);
```

Additionally, `PostgresEventStore` expose `loadEventsByQuery` method, that allows to query events based on several fields:

* `system_id`
* `user_id`
* `emission_date`

If you plan to use this method, you should also index these columns.

```sql
CREATE INDEX app_user_id_idx ON app_journal (user_id);
CREATE INDEX app_system_id_idx ON app_journal (system_id);
CREATE INDEX app_emission_date_idx ON app_journal (emission_date);
```
