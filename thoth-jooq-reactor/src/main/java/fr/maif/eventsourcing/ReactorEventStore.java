package fr.maif.eventsourcing;

import io.vavr.Tuple0;
import io.vavr.collection.List;
import io.vavr.control.Option;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import scala.util.hashing.MurmurHash3$;

import java.util.concurrent.atomic.LongAccumulator;
import java.util.function.BiFunction;
import java.util.function.Function;


public interface ReactorEventStore<TxCtx, E extends Event, Meta, Context> {

    Mono<Tuple0> persist(TxCtx transactionContext, List<EventEnvelope<E, Meta, Context>> events);

    Flux<EventEnvelope<E, Meta, Context>> loadEventsUnpublished(TxCtx tx, EventStore.ConcurrentReplayStrategy concurrentReplayStrategy);

    Flux<EventEnvelope<E, Meta, Context>> loadEventsByQuery(TxCtx tx, EventStore.Query query);

    Flux<EventEnvelope<E, Meta, Context>> loadEventsByQuery(EventStore.Query query);

    default Flux<EventEnvelope<E, Meta, Context>> loadEvents(String id) {
        return loadEventsByQuery(EventStore.Query.builder().withEntityId(id).build());
    }

    default Flux<EventEnvelope<E, Meta, Context>> loadAllEvents() {
        return loadEventsByQuery(EventStore.Query.builder().build());
    }

    Mono<Long> nextSequence(TxCtx tx);

    Mono<Tuple0> publish(List<EventEnvelope<E, Meta, Context>> events);

    Mono<EventEnvelope<E, Meta, Context>> markAsPublished(TxCtx tx, EventEnvelope<E, Meta, Context> eventEnvelope);

    default Mono<List<EventEnvelope<E, Meta, Context>>> markAsPublished(TxCtx tx, List<EventEnvelope<E, Meta, Context>> eventEnvelopes) {
        return Flux.fromIterable(eventEnvelopes)
                .concatMap(evt -> this.markAsPublished(tx, evt))
                .collectList()
                .map(List::ofAll);
    }

    Mono<EventEnvelope<E, Meta, Context>> markAsPublished(EventEnvelope<E, Meta, Context> eventEnvelope);

    default Mono<List<EventEnvelope<E, Meta, Context>>> markAsPublished(List<EventEnvelope<E, Meta, Context>> eventEnvelopes) {
        return Flux.fromIterable(eventEnvelopes)
                .concatMap(this::markAsPublished)
                .collectList()
                .map(List::ofAll);
    }

    Mono<TxCtx> openTransaction();

    Mono<Tuple0> commitOrRollback(Option<Throwable> of, TxCtx tx);

    /**
     * Stream elements from journal and execute an handling function concurrently.
     * The function shard by entity id, so event for the same entity won't be handled concurrently.
     *
     * @param fromSequenceNum sequence num to start with
     * @param parallelism concurrent factor
     * @param pageSize limit to n events
     * @param handle the handling fonction for example to build a new projection
     * @return the last sequence num handled
     */
    default Mono<Long> concurrentReplayPage(Long fromSequenceNum, Integer parallelism, Integer pageSize, BiFunction<Integer, Flux<EventEnvelope<E, Meta, Context>>, Mono<Tuple0>> handle) {
        LongAccumulator lastSeqNum = new LongAccumulator(Long::max, 0);
        return this.loadEventsByQuery(EventStore.Query.builder().withSequenceFrom(fromSequenceNum).withSize(pageSize).build())
                .groupBy(evt -> Math.abs(MurmurHash3$.MODULE$.stringHash(evt.entityId)) % parallelism)
                .flatMap(flux  -> handle.apply(flux.key(), flux.doOnNext(evt -> lastSeqNum.accumulate(evt.sequenceNum))), parallelism)
                .last()
                .map(any -> lastSeqNum.get());
    }

    /**
     * Stream all elements from journal and execute an handling function concurrently.
     * The function shard by entity id, so event for the same entity won't be handled concurrently.
     *
     * @param parallelism concurrent factor
     * @param pageSize number of event in memory
     * @param handle the handling fonction for example to build a new projection
     * @return the last sequence num handled
     */
    default Mono<Long> concurrentReplay(Integer parallelism, Integer pageSize, BiFunction<Integer, Flux<EventEnvelope<E, Meta, Context>>, Mono<Tuple0>> handle) {
        Function<Long, Mono<Long>> run = n -> concurrentReplayPage(n, parallelism, pageSize, handle);
        return run.apply(0L).expand(run).last();
    }

    EventStore<TxCtx, E, Meta, Context> toEventStore();

    static <TxCtx, E extends Event, Meta, Context> ReactorEventStore<TxCtx, E, Meta, Context> fromEventStore(EventStore<TxCtx, E, Meta, Context> eventStore) {
        return new DefaultReactorEventStore<>(eventStore);
    }

}
