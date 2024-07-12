package fr.maif.reactor.eventsourcing;

import fr.maif.concurrent.CompletionStages;
import fr.maif.eventsourcing.Event;
import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.eventsourcing.EventPublisher;
import fr.maif.eventsourcing.EventStore;
import io.vavr.API;
import io.vavr.Tuple;
import io.vavr.Tuple0;
import io.vavr.collection.List;
import io.vavr.collection.Stream;
import io.vavr.control.Option;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public class InMemoryEventStore<E extends Event, Meta, Context> implements EventStore<InMemoryEventStore.Transaction<E, Meta, Context>, E, Meta, Context> {

    ConcurrentHashMap<Long, EventEnvelope<E, Meta, Context>> store = new ConcurrentHashMap<>();

    AtomicLong sequenceNums = new AtomicLong(0);
    private final Supplier<CompletionStage<Tuple0>> markAsPublishedTx;
    private final Supplier<CompletionStage<Tuple0>> markAsPublished;

    private final static Supplier<CompletionStage<Tuple0>> NOOP = () -> CompletionStages.completedStage(Tuple.empty());

    public InMemoryEventStore(Supplier<CompletionStage<Tuple0>> markAsPublishedTx,
                              Supplier<CompletionStage<Tuple0>> markAsPublished,
                              EventEnvelope<E, Meta, Context>... events) {

        this.markAsPublishedTx = markAsPublishedTx;
        this.markAsPublished = markAsPublished;
        Stream.of(events).forEach(e -> store.put(e.sequenceNum, e));
    }

    public InMemoryEventStore(EventEnvelope<E, Meta, Context>... events) {
        this(NOOP, NOOP, events);
    }

    @SafeVarargs
    public static <E extends Event, Meta, Context> InMemoryEventStore<E, Meta, Context> create(EventEnvelope<E, Meta, Context>... events) {
        return new InMemoryEventStore<>(events);
    }


    public record Transaction<E extends Event, Meta, Context>(ArrayList<EventEnvelope<E, Meta, Context>> events,
                                                              ArrayList<EventEnvelope<E, Meta, Context>> toPublish) {

        public Transaction() {
            this(new ArrayList<>(), new ArrayList<>());
        }

        public static <E extends Event, Meta, Context> Transaction<E, Meta, Context> newTx() {
            return new Transaction<>();
        }

        public Tuple0 addAll(java.util.List<EventEnvelope<E, Meta, Context>> events) {
            this.events.addAll(events);
            return API.Tuple();
        }
    }

    @Override
    public Publisher<EventEnvelope<E, Meta, Context>> loadEventsUnpublished(Transaction<E, Meta, Context> tx, ConcurrentReplayStrategy concurrentReplayStrategy) {
        return Flux.fromIterable(store.values().stream().sorted(Comparator.comparingLong(e -> e.sequenceNum)).toList())
                .filter(e -> Boolean.FALSE.equals(e.published));
    }

    @Override
    public CompletionStage<Long> lastPublishedSequence() {
        AtomicLong max = new AtomicLong(0L);
        store.values().forEach(k -> {
            if (k.published) {
                max.accumulateAndGet(k.sequenceNum, Math::max);
            }
        });
        return CompletionStages.completedStage(max.get());
    }

    @Override
    public CompletionStage<Transaction<E, Meta, Context>> openTransaction() {
        return CompletionStages.completedStage(new Transaction<>());
    }

    @Override
    public CompletionStage<EventEnvelope<E, Meta, Context>> markAsPublished(Transaction<E, Meta, Context> tx, EventEnvelope<E, Meta, Context> eventEnvelope) {
        return markAsPublishedTx.get().thenCompose(any -> {
            tx.toPublish().add(eventEnvelope);
            return CompletionStages.completedStage(eventEnvelope);
        });
    }

    @Override
    public CompletionStage<EventEnvelope<E, Meta, Context>> markAsPublished(EventEnvelope<E, Meta, Context> eventEnvelope) {
        return markAsPublished.get().thenCompose(any ->
                CompletionStages.completedStage(store.compute(eventEnvelope.sequenceNum, (k, event) -> {
                    if (event == null) {
                        return eventEnvelope.copy().withPublished(true).build();
                    } else {
                        return event.copy().withPublished(true).build();
                    }
                }))
        );
    }

    @Override
    public CompletionStage<Tuple0> persist(Transaction<E, Meta, Context> transactionContext, List<EventEnvelope<E, Meta, Context>> events) {
        return CompletionStages.completedStage(transactionContext.addAll(events.toJavaList()));
    }

    @Override
    public CompletionStage<Tuple0> commitOrRollback(Option<Throwable> of, Transaction<E, Meta, Context> tx) {
        if (of.isEmpty()) {
            tx.events().forEach(event ->
                    store.put(event.sequenceNum, event)
            );
            tx.toPublish.forEach(e ->
                    store.computeIfPresent(e.sequenceNum, (k, event) -> event.copy().withPublished(true).build())
            );
        }
        tx.events.clear();
        tx.toPublish.clear();
        return CompletionStages.completedStage(API.Tuple());
    }

    @Override
    public CompletionStage<Long> nextSequence(InMemoryEventStore.Transaction<E, Meta, Context> tx) {
        long value = store.values().stream().map(e -> e.sequenceNum).max(Comparator.comparingLong(e -> e)).orElse(0L) + 1;
        sequenceNums.incrementAndGet();
        return CompletionStages.completedStage(sequenceNums.accumulateAndGet(value, Math::max));
    }

    @Override
    public CompletionStage<List<Long>> nextSequences(InMemoryEventStore.Transaction<E, Meta, Context> tx, Integer count) {
        return CompletionStages.traverse(List.range(0, count), c -> nextSequence(tx));
    }

    @Override
    public CompletionStage<Tuple0> publish(List<EventEnvelope<E, Meta, Context>> events) {
        events.forEach(e -> store.put(e.sequenceNum, e));
        return CompletionStages.completedStage(API.Tuple());
    }

    @Override
    public Publisher<EventEnvelope<E, Meta, Context>> loadEvents(String id) {
        return Flux.fromIterable(store.values()).filter(e ->
                e.entityId.equals(id)
        );
    }


    @Override
    public Publisher<EventEnvelope<E, Meta, Context>> loadAllEvents() {
        return Flux.fromIterable(store.values());
    }

    @Override
    public Publisher<EventEnvelope<E, Meta, Context>> loadEventsByQuery(InMemoryEventStore.Transaction<E, Meta, Context> tx, Query query) {
        return loadEventsByQuery(query);
    }

    @Override
    public Publisher<EventEnvelope<E, Meta, Context>> loadEventsByQuery(Query query) {
        return Flux.fromIterable(store.values())
                .filter(e -> Option.of(query.entityId).map(id -> id.equals(e.entityId)).getOrElse(true));
    }


    @Override
    public EventPublisher<E, Meta, Context> eventPublisher() {
        var _this = this;
        return new EventPublisher<E, Meta, Context>() {
            @Override
            public CompletionStage<Tuple0> publish(List<EventEnvelope<E, Meta, Context>> events) {
                return _this.publish(events);
            }

            @Override
            public void close() throws IOException {

            }
        };
    }
}
