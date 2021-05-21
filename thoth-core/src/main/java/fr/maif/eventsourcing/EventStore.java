package fr.maif.eventsourcing;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.Materializer;
import akka.stream.javadsl.Source;
import io.vavr.Tuple0;
import io.vavr.Value;
import io.vavr.collection.List;
import io.vavr.concurrent.Future;
import io.vavr.control.Option;

import java.time.LocalDateTime;

public interface EventStore<TxCtx, E extends Event, Meta, Context> {

    ActorSystem system();

    Materializer materializer();

    Future<Tuple0> persist(TxCtx transactionContext, List<EventEnvelope<E, Meta, Context>> events);

    Source<EventEnvelope<E, Meta, Context>, NotUsed> loadEventsUnpublished(TxCtx tx, ConcurrentReplayStrategy concurrentReplayStrategy);

    Source<EventEnvelope<E, Meta, Context>, NotUsed> loadEventsByQuery(TxCtx tx, Query query);

    Source<EventEnvelope<E, Meta, Context>, NotUsed> loadEventsByQuery(Query query);

    default Source<EventEnvelope<E, Meta, Context>, NotUsed> loadEvents(String id) {
        return loadEventsByQuery(Query.builder().withEntityId(id).build());
    }

    default Source<EventEnvelope<E, Meta, Context>, NotUsed> loadAllEvents(){
        return loadEventsByQuery(Query.builder().build());
    }

    Future<Long> nextSequence(TxCtx tx);

    Future<Tuple0> publish(List<EventEnvelope<E, Meta, Context>> events);


    Future<EventEnvelope<E, Meta, Context>> markAsPublished(TxCtx tx, EventEnvelope<E, Meta, Context> eventEnvelope);

    default Future<List<EventEnvelope<E, Meta, Context>>> markAsPublished(TxCtx tx, List<EventEnvelope<E, Meta, Context>> eventEnvelopes) {
        return Future.traverse(eventEnvelopes, evt -> this.markAsPublished(tx, evt)).map(Value::toList);
    }

    Future<EventEnvelope<E, Meta, Context>> markAsPublished(EventEnvelope<E, Meta, Context> eventEnvelope);

    default Future<List<EventEnvelope<E, Meta, Context>>> markAsPublished(List<EventEnvelope<E, Meta, Context>> eventEnvelopes) {
        return Future.traverse(eventEnvelopes, this::markAsPublished).map(Value::toList);
    }

    Future<TxCtx> openTransaction();

    Future<Tuple0> commitOrRollback(Option<Throwable> of, TxCtx tx);

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
