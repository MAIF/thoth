package fr.maif.eventsourcing.impl;

import akka.actor.ActorSystem;
import akka.stream.javadsl.AsPublisher;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import fr.maif.Helpers;
import fr.maif.Helpers.Viking;
import fr.maif.Helpers.VikingEvent;
import fr.maif.akka.eventsourcing.DefaultAggregateStore;
import fr.maif.concurrent.CompletionStages;
import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.eventsourcing.EventProcessorTest;
import fr.maif.eventsourcing.EventStore;
import fr.maif.eventsourcing.EventStore.Query;
import fr.maif.eventsourcing.ReadConcurrencyStrategy;
import io.vavr.Tuple;
import io.vavr.Tuple0;
import io.vavr.collection.List;
import io.vavr.control.Option;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

import static io.vavr.API.Some;
import static io.vavr.API.Tuple;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;


class DefaultAggregateStoreTest {

    static ActorSystem actorSystem = ActorSystem.create();
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

        EventStore<Tuple0, VikingEvent, Tuple0, Tuple0> eventStore = mock(EventStore.class);
        DefaultAggregateStore<Viking, VikingEvent, Tuple0, Tuple0, Tuple0> aggregateStore = new DefaultAggregateStore<>(eventStore, new Helpers.VikingEventHandler(), actorSystem, new EventProcessorTest.FakeTransactionManager(), ReadConcurrencyStrategy.NO_STRATEGY);

        Query query = Query.builder().withEntityId(entityId).build();
        when(eventStore.loadEventsByQuery(Tuple(), query)).thenReturn(Source.from(List.of(eventEnvelope1, eventEnvelope2)).runWith(Sink.asPublisher(AsPublisher.WITHOUT_FANOUT), actorSystem));

        Option<Viking> vikings = aggregateStore.getAggregate(Tuple.empty(), entityId).toCompletableFuture().join();

        Assertions.assertThat(vikings).isEqualTo(Some(new Viking(entityId, "Ragnar Lodbrock", 2L)));
        verify(eventStore, times(1)).loadEventsByQuery(Tuple(), query);
    }

    @Test
    void testReloadEventAndBuildAggregateWithSnapshots() {

        EventStore<Tuple0, VikingEvent, Tuple0, Tuple0> eventStore = mock(EventStore.class);
        DefaultAggregateStore<Viking, VikingEvent, Tuple0, Tuple0, Tuple0> aggregateStore = spy(new DefaultAggregateStore<Viking, VikingEvent, Tuple0, Tuple0, Tuple0>(eventStore, new Helpers.VikingEventHandler(), actorSystem, new EventProcessorTest.FakeTransactionManager(), ReadConcurrencyStrategy.NO_STRATEGY) {
            @Override
            public CompletionStage<Option<Viking>> getSnapshot(Tuple0 transactionContext, String id) {
                return CompletionStages.successful(Option.some(new Viking(id, "Rollo", 1L)));
            }
        });

        Query query = Query.builder().withEntityId(entityId).withSequenceFrom(1L).build();
        when(eventStore.loadEventsByQuery(Tuple(), query)).thenReturn(Source.from(List.of(eventEnvelope2)).runWith(Sink.asPublisher(AsPublisher.WITHOUT_FANOUT), actorSystem));

        Option<Viking> vikings = aggregateStore.getAggregate(Tuple.empty(), entityId).toCompletableFuture().join();

        Assertions.assertThat(vikings).isEqualTo(Some(new Viking(entityId, "Ragnar Lodbrock", 2L)));
        verify(eventStore, times(1)).loadEventsByQuery(Tuple(), query);
        verify(aggregateStore, times(1)).getSnapshot(any(), eq(entityId));
    }
}