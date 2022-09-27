package fr.maif.eventsourcing;

import fr.maif.concurrent.CompletionStages;
import io.vavr.Tuple0;
import io.vavr.Value;
import io.vavr.collection.List;
import io.vavr.control.Option;
import org.reactivestreams.Publisher;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

public interface EventStore<TxCtx, E extends Event, Meta, Context> {

    CompletionStage<Void> persist(TxCtx transactionContext, List<EventEnvelope<E, Meta, Context>> events);

    Publisher<EventEnvelope<E, Meta, Context>> loadEventsUnpublished(TxCtx tx, ConcurrentReplayStrategy concurrentReplayStrategy);

    Publisher<EventEnvelope<E, Meta, Context>> loadEventsByQuery(TxCtx tx, Query query);

    Publisher<EventEnvelope<E, Meta, Context>> loadEventsByQuery(Query query);

    default Publisher<EventEnvelope<E, Meta, Context>> loadEvents(String id) {
        return loadEventsByQuery(Query.builder().withEntityId(id).build());
    }

    default Publisher<EventEnvelope<E, Meta, Context>> loadAllEvents() {
        return loadEventsByQuery(Query.builder().build());
    }

    CompletionStage<Long> nextSequence(TxCtx tx);

    CompletionStage<Void> publish(List<EventEnvelope<E, Meta, Context>> events);

    CompletionStage<EventEnvelope<E, Meta, Context>> markAsPublished(TxCtx tx, EventEnvelope<E, Meta, Context> eventEnvelope);

    default CompletionStage<List<EventEnvelope<E, Meta, Context>>> markAsPublished(TxCtx tx, List<EventEnvelope<E, Meta, Context>> eventEnvelopes) {
        return CompletionStages.traverse(eventEnvelopes, evt -> this.markAsPublished(tx, evt));
    }

    CompletionStage<EventEnvelope<E, Meta, Context>> markAsPublished(EventEnvelope<E, Meta, Context> eventEnvelope);

    default CompletionStage<List<EventEnvelope<E, Meta, Context>>> markAsPublished(List<EventEnvelope<E, Meta, Context>> eventEnvelopes) {
        return CompletionStages.traverse(eventEnvelopes, this::markAsPublished);
    }

    CompletionStage<TxCtx> openTransaction();

    CompletionStage<Void> commitOrRollback(Option<Throwable> of, TxCtx tx);

    /**
     * Strategy to choose when replaying journal in case of crash when there is two or more nodes that want to replay concurrently.
     * <ul>
     *  <li>SKIP : if two node replay at the same time, the second will not see events to replay. The impact, is that the new events on that node will be sent to kafka before the replay is finished</li>
     *  <li>NO_STRATEGY : all nodes will replay, so the events will be send multiple times</li>
     *  <li>WAIT : on node will replay and the other will be block, waiting for the replay to finish. The new events will be kept in memory and will be sent at the end of the replay</li>
     * </ul>
     */
    enum ConcurrentReplayStrategy {
        SKIP, WAIT, NO_STRATEGY
    }

    class Query {

        public final LocalDateTime dateFrom;
        public final LocalDateTime dateTo;
        public final String entityId;
        public final Integer size;
        public final String userId;
        public final String systemId;
        public final Long sequenceFrom;
        public final Long sequenceTo;
        public final Boolean published;

        private Query(Query.Builder builder) {
            this.dateFrom = builder.dateFrom;
            this.dateTo = builder.dateTo;
            this.entityId = builder.entityId;
            this.size = builder.size;
            this.userId = builder.userId;
            this.systemId = builder.systemId;
            this.published = builder.published;
            this.sequenceFrom = builder.sequenceFrom;
            this.sequenceTo = builder.sequenceTo;
        }

        public static Builder builder() {
            return new Builder();
        }

        public Option<LocalDateTime> dateFrom() {
            return Option.of(dateFrom);
        }

        public Option<LocalDateTime> dateTo() {
            return Option.of(dateTo);
        }

        public Option<String> entityId() {
            return Option.of(entityId);
        }

        public Option<String> userId() {
            return Option.of(userId);
        }

        public Option<String> systemId() {
            return Option.of(systemId);
        }

        public Option<Boolean> published() {
            return Option.of(published);
        }

        public Option<Long> sequenceFrom() {
            return Option.of(sequenceFrom);
        }

        public Option<Long> sequenceTo() {
            return Option.of(sequenceTo);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Query query = (Query) o;
            return Objects.equals(dateFrom, query.dateFrom) &&
                    Objects.equals(dateTo, query.dateTo) &&
                    Objects.equals(entityId, query.entityId) &&
                    Objects.equals(size, query.size) &&
                    Objects.equals(userId, query.userId) &&
                    Objects.equals(systemId, query.systemId) &&
                    Objects.equals(sequenceFrom, query.sequenceFrom) &&
                    Objects.equals(sequenceTo, query.sequenceTo) &&
                    Objects.equals(published, query.published);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dateFrom, dateTo, entityId, size, userId, systemId, sequenceFrom, sequenceTo, published);
        }

        public static class Builder {
            LocalDateTime dateFrom;
            LocalDateTime dateTo;
            String entityId;
            Integer size;
            String userId;
            String systemId;
            Boolean published;
            Long sequenceFrom;
            Long sequenceTo;

            public Builder withDateFrom(LocalDateTime dateFrom) {
                this.dateFrom = dateFrom;
                return this;
            }

            public Builder withDateTo(LocalDateTime dateTo) {
                this.dateTo = dateTo;
                return this;
            }

            public Builder withEntityId(String entityId) {
                this.entityId = entityId;
                return this;
            }

            public Builder withSize(Integer size) {
                this.size = size;
                return this;
            }

            public Builder withUserId(String userId) {
                this.userId = userId;
                return this;
            }

            public Builder withSystemId(String systemId) {
                this.systemId = systemId;
                return this;
            }

            public Builder withPublished(Boolean published) {
                this.published = published;
                return this;
            }

            public Builder withSequenceFrom(Long sequenceFrom) {
                this.sequenceFrom = sequenceFrom;
                return this;
            }

            public Builder withSequenceTo(Long sequenceTo) {
                this.sequenceTo = sequenceTo;
                return this;
            }

            public Query build() {
                return new Query(this);
            }
        }

    }
}
