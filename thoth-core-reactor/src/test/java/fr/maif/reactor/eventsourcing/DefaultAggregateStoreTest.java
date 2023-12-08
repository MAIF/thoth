package fr.maif.reactor.eventsourcing;

import fr.maif.Helpers;
import fr.maif.Helpers.Viking;
import fr.maif.Helpers.VikingEvent;
import fr.maif.concurrent.CompletionStages;
import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.eventsourcing.EventStore;
import fr.maif.eventsourcing.EventStore.Query;
import fr.maif.reactor.eventsourcing.InMemoryEventStore.Transaction;
import io.vavr.Tuple;
import io.vavr.Tuple0;
import io.vavr.collection.List;
import io.vavr.control.Option;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

import static io.vavr.API.Some;
import static io.vavr.API.Tuple;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


class DefaultAggregateStoreTest {

    final String entityId = "1";

    EventEnvelope<VikingEvent, Tuple0, Tuple0> eventEnvelope1 = EventEnvelope.<VikingEvent, Tuple0, Tuple0>builder()
            .withEmissionDate(LocalDateTime.now())
            .withId(UUID.randomUUID())
            .withEntityId(entityId)
            .withSequenceNum(1L)
            .withEventType(VikingEvent.VikingCreatedV1.name())
            .withVersion(VikingEvent.VikingCreatedV1.version())
            .withTotalMessageInTransaction(1)
            .withNumMessageInTransaction(1)
            .withTransactionId(UUID.randomUUID().toString())
            .withEvent(new VikingEvent.VikingCreated(entityId, "ragnar"))
            .build();

    EventEnvelope<VikingEvent, Tuple0, Tuple0> eventEnvelope2 = EventEnvelope.<VikingEvent, Tuple0, Tuple0>builder()
            .withEmissionDate(LocalDateTime.now())
            .withId(UUID.randomUUID())
            .withEntityId(entityId)
            .withSequenceNum(2L)
            .withEventType(VikingEvent.VikingUpdatedV1.name())
            .withVersion(VikingEvent.VikingUpdatedV1.version())
            .withTotalMessageInTransaction(1)
            .withNumMessageInTransaction(1)
            .withTransactionId(UUID.randomUUID().toString())
            .withEvent(new VikingEvent.VikingUpdated(entityId, "Ragnar Lodbrock"))
            .build();

    @Test
    void testReloadEventAndBuildAggregateWithoutSnapshots() {

        EventStore<Transaction<VikingEvent, Tuple0, Tuple0>, VikingEvent, Tuple0, Tuple0> eventStore = mock(EventStore.class);
        var aggregateStore = new DefaultAggregateStore<>(eventStore, new Helpers.VikingEventHandler(), new EventProcessorTest.FakeTransactionManager());

        Query query = Query.builder().withEntityId(entityId).build();
        when(eventStore.loadEventsByQuery(any(), eq(query))).thenReturn(Flux.fromIterable(List.of(eventEnvelope1, eventEnvelope2)));

        Option<Viking> vikings = aggregateStore.getAggregate(Transaction.newTx(), entityId).toCompletableFuture().join();

        Assertions.assertThat(vikings).isEqualTo(Some(new Viking(entityId, "Ragnar Lodbrock", 30, 2L)));
        verify(eventStore, times(1)).loadEventsByQuery(any(), eq(query));
    }

    @Test
    void testReloadEventAndBuildAggregateWithSnapshots() {

        EventStore<Transaction<VikingEvent, Tuple0, Tuple0>, VikingEvent, Tuple0, Tuple0> eventStore = mock(EventStore.class);
        DefaultAggregateStore<Viking, VikingEvent, Tuple0, Tuple0, Transaction<VikingEvent, Tuple0, Tuple0>> aggregateStore = spy(new DefaultAggregateStore<Viking, VikingEvent, Tuple0, Tuple0, Transaction<VikingEvent, Tuple0, Tuple0>>(eventStore, new Helpers.VikingEventHandler(), new EventProcessorTest.FakeTransactionManager()) {
            @Override
            public CompletionStage<Option<Viking>> getSnapshot(Transaction<VikingEvent, Tuple0, Tuple0> transactionContext, String id) {
                return CompletionStages.successful(Option.some(new Viking(id, "Rollo", 30, 1L)));
            }
        });

        Query query = Query.builder().withEntityId(entityId).withSequenceFrom(1L).build();
        when(eventStore.loadEventsByQuery(any(), eq(query))).thenReturn(Flux.fromIterable(List.of(eventEnvelope2)));

        Option<Viking> vikings = aggregateStore.getAggregate(Transaction.newTx(), entityId).toCompletableFuture().join();

        Assertions.assertThat(vikings).isEqualTo(Some(new Viking(entityId, "Ragnar Lodbrock", 30, 2L)));
        verify(eventStore, times(1)).loadEventsByQuery(any(), eq(query));
        verify(aggregateStore, times(1)).getSnapshot(any(), eq(entityId));
    }
}