package fr.maif.reactor.eventsourcing.vanilla;

import fr.maif.concurrent.CompletionStages;
import fr.maif.eventsourcing.Event;
import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.eventsourcing.Unit;
import fr.maif.eventsourcing.vanilla.EventPublisher;
import fr.maif.eventsourcing.vanilla.EventStore;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class InMemoryEventStore<E extends Event, Meta, Context> implements EventStore<InMemoryEventStore.Transaction<E, Meta, Context>, E, Meta, Context> {

    ConcurrentHashMap<Long, EventEnvelope<E, Meta, Context>> store = new ConcurrentHashMap<>();

    AtomicLong sequenceNums = new AtomicLong(0);
    private final Supplier<CompletionStage<Unit>> markAsPublishedTx;
    private final Supplier<CompletionStage<Unit>> markAsPublished;

    private final static Supplier<CompletionStage<Unit>> NOOP = () -> CompletionStages.completedStage(Unit.unit());

    public InMemoryEventStore(Supplier<CompletionStage<Unit>> markAsPublishedTx,
                              Supplier<CompletionStage<Unit>> markAsPublished,
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

        public Unit addAll(java.util.List<EventEnvelope<E, Meta, Context>> events) {
            this.events.addAll(events);
            return Unit.unit();
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
    public CompletionStage<Void> persist(Transaction<E, Meta, Context> transactionContext, List<EventEnvelope<E, Meta, Context>> events) {
        transactionContext.addAll(events);
        return CompletableFuture.runAsync(() -> {});
    }

    @Override
    public CompletionStage<Void> commitOrRollback(Optional<Throwable> of, Transaction<E, Meta, Context> tx) {
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
        return CompletableFuture.runAsync(() -> {});
    }

    @Override
    public CompletionStage<Long> nextSequence(Transaction<E, Meta, Context> tx) {
        long value = store.values().stream().map(e -> e.sequenceNum).max(Comparator.comparingLong(e -> e)).orElse(0L) + 1;
        sequenceNums.incrementAndGet();
        return CompletionStages.completedStage(sequenceNums.accumulateAndGet(value, Math::max));
    }

    @Override
    public CompletionStage<List<Long>> nextSequences(Transaction<E, Meta, Context> tx, Integer count) {
        return CompletionStages.traverse(IntStream.range(0, count).boxed(), c -> nextSequence(tx)).thenApply(l -> l.toJavaList());
    }

    @Override
    public CompletionStage<Void> publish(List<EventEnvelope<E, Meta, Context>> events) {
        events.forEach(e -> store.put(e.sequenceNum, e));
        return CompletableFuture.runAsync(() -> {});
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
    public Publisher<EventEnvelope<E, Meta, Context>> loadEventsByQuery(Transaction<E, Meta, Context> tx, Query query) {
        return loadEventsByQuery(query);
    }

    @Override
    public Publisher<EventEnvelope<E, Meta, Context>> loadEventsByQuery(Query query) {
        return Flux.fromIterable(store.values())
                .filter(e -> Optional.ofNullable(query.entityId).map(id -> id.equals(e.entityId)).orElse(true));
    }


    @Override
    public EventPublisher<E, Meta, Context> eventPublisher() {
        var _this = this;
        return new EventPublisher<E, Meta, Context>() {
            @Override
            public CompletionStage<Void> publish(List<EventEnvelope<E, Meta, Context>> events) {
                return _this.publish(events);
            }

            @Override
            public <TxCtx> CompletionStage<Void> publishNonAcknowledgedFromDb(EventStore<TxCtx, E, Meta, Context> eventStore, ConcurrentReplayStrategy concurrentReplayStrategy) {
                return CompletableFuture.runAsync(() -> {});
            }

            @Override
            public void close() throws IOException {

            }
        };
    }
}
