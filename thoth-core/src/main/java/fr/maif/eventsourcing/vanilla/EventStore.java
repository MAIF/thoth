package fr.maif.eventsourcing.vanilla;

import fr.maif.eventsourcing.Event;
import fr.maif.eventsourcing.EventEnvelope;
import org.reactivestreams.Publisher;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

public interface EventStore<TxCtx, E extends Event, Meta, Context> {

    CompletionStage<Void> persist(TxCtx transactionContext, List<EventEnvelope<E, Meta, Context>> events);

    CompletionStage<Long> lastPublishedSequence();

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

    CompletionStage<List<Long>> nextSequences(TxCtx tx, Integer count);

    CompletionStage<Void> publish(List<EventEnvelope<E, Meta, Context>> events);

    CompletionStage<EventEnvelope<E, Meta, Context>> markAsPublished(TxCtx tx, EventEnvelope<E, Meta, Context> eventEnvelope);

    CompletionStage<List<EventEnvelope<E, Meta, Context>>> markAsPublished(TxCtx tx, List<EventEnvelope<E, Meta, Context>> eventEnvelopes);

    CompletionStage<EventEnvelope<E, Meta, Context>> markAsPublished(EventEnvelope<E, Meta, Context> eventEnvelope);

    CompletionStage<List<EventEnvelope<E, Meta, Context>>> markAsPublished(List<EventEnvelope<E, Meta, Context>> eventEnvelopes);

    CompletionStage<TxCtx> openTransaction();

    CompletionStage<Void> commitOrRollback(Optional<Throwable> of, TxCtx tx);

    EventPublisher<E, Meta, Context> eventPublisher();

    record IdAndSequence(String id, Long sequence) {}

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
        public final List<IdAndSequence> idsAndSequences;

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
            this.idsAndSequences = Objects.requireNonNullElse(builder.idsAndSequences, List.of());
        }

        public static EventStore.Query.Builder builder() {
            return new EventStore.Query.Builder();
        }

        public Optional<LocalDateTime> dateFrom() {
            return Optional.ofNullable(dateFrom);
        }

        public Optional<LocalDateTime> dateTo() {
            return Optional.ofNullable(dateTo);
        }

        public Optional<String> entityId() {
            return Optional.ofNullable(entityId);
        }

        public Optional<String> userId() {
            return Optional.ofNullable(userId);
        }

        public Optional<String> systemId() {
            return Optional.ofNullable(systemId);
        }

        public Optional<Boolean> published() {
            return Optional.ofNullable(published);
        }

        public Optional<Long> sequenceFrom() {
            return Optional.ofNullable(sequenceFrom);
        }

        public Optional<Long> sequenceTo() {
            return Optional.ofNullable(sequenceTo);
        }

        public List<IdAndSequence> idsAndSequences() {
            return idsAndSequences;
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
            List<IdAndSequence> idsAndSequences;

            public Query.Builder withDateFrom(LocalDateTime dateFrom) {
                this.dateFrom = dateFrom;
                return this;
            }

            public Query.Builder withDateTo(LocalDateTime dateTo) {
                this.dateTo = dateTo;
                return this;
            }

            public Query.Builder withEntityId(String entityId) {
                this.entityId = entityId;
                return this;
            }

            public Query.Builder withSize(Integer size) {
                this.size = size;
                return this;
            }

            public Query.Builder withUserId(String userId) {
                this.userId = userId;
                return this;
            }

            public Query.Builder withSystemId(String systemId) {
                this.systemId = systemId;
                return this;
            }

            public Query.Builder withPublished(Boolean published) {
                this.published = published;
                return this;
            }

            public Query.Builder withSequenceFrom(Long sequenceFrom) {
                this.sequenceFrom = sequenceFrom;
                return this;
            }

            public Query.Builder withSequenceTo(Long sequenceTo) {
                this.sequenceTo = sequenceTo;
                return this;
            }
            public Query.Builder withIdsAndSequences(List<IdAndSequence> idsAndSequences) {
                this.idsAndSequences = idsAndSequences;
                return this;
            }

            public Query build() {
                return new Query(this);
            }
        }

    }
}
