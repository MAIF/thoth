package fr.maif.eventsourcing;

import akka.actor.ActorSystem;
import akka.stream.Materializer;
import akka.stream.javadsl.Sink;
import akka.testkit.javadsl.TestKit;
import io.vavr.Tuple;
import io.vavr.Tuple0;
import fr.maif.eventsourcing.impl.InMemoryEventStore;
import io.vavr.collection.List;
import io.vavr.concurrent.Future;
import io.vavr.control.Either;
import io.vavr.control.Option;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static fr.maif.Helpers.*;
import static fr.maif.Helpers.VikingCommand.*;
import static io.vavr.API.Some;
import static org.assertj.core.api.Assertions.assertThat;

public class EventProcessorTest {

    private ActorSystem actorSystem;

    @BeforeEach
    public void setUp() {
        actorSystem = ActorSystem.create();
    }


    @AfterEach
    public void cleanUp() {
        TestKit.shutdownActorSystem(actorSystem);
    }


    @Test
    public void oneCommandShouldGenerateEventAndPersistState() {

        //Set up
        VikingSnapshot vikingSnapshot = new VikingSnapshot();
        InMemoryEventStore<Tuple0, VikingEvent, Tuple0, Tuple0> inMemoryEventStore = InMemoryEventStore.create(actorSystem);
        EventProcessor<String, Viking, VikingCommand, VikingEvent, Tuple0, String, Tuple0, Tuple0>  vikingEventProcessor = vikingEventProcessor(inMemoryEventStore, vikingSnapshot);

        //Test
        Either<String, ProcessingSuccess<Viking, VikingEvent, Tuple0, Tuple0, String>> result = vikingEventProcessor.processCommand(new CreateViking("1", "ragnar")).get();

        // Results
        assertThat(result.isRight()).isTrue();
        ProcessingSuccess<Viking, VikingEvent, Tuple0, Tuple0, String> eventAndState = result.get();
        assertThat(eventAndState.getPreviousState()).isEqualTo(Option.none());
        Viking expected = new Viking("1", "ragnar", 1L);
        assertThat(eventAndState.getCurrentState()).isEqualTo(Option.some(expected));

        java.util.List<EventEnvelope<VikingEvent, Tuple0, Tuple0>> eventsFromJournal = inMemoryEventStore.loadAllEvents().runWith(Sink.seq(), Materializer.createMaterializer(actorSystem)).toCompletableFuture().join();

        EventEnvelope<VikingEvent, Tuple0, Tuple0> expectedEnvelope = EventEnvelope.<VikingEvent, Tuple0, Tuple0>builder()
                .withEmissionDate(eventsFromJournal.get(0).emissionDate)
                .withId(eventsFromJournal.get(0).id)
                .withEntityId("1")
                .withSequenceNum(1L)
                .withEventType(VikingEvent.VikingCreatedV1.name())
                .withVersion(VikingEvent.VikingCreatedV1.version())
                .withTotalMessageInTransaction(1)
                .withNumMessageInTransaction(1)
                .withTransactionId("1")
                .withEvent(new VikingEvent.VikingCreated("1", "ragnar"))
                .build();
        assertThat(eventAndState.getEvents()).containsExactly(expectedEnvelope);
        assertThat(eventAndState.getMessage()).isEqualTo("C");

        assertThat(eventsFromJournal).containsExactly(expectedEnvelope);

        assertThat(vikingSnapshot.data.get("1")).isEqualTo(expected);
    }


    @Test
    public void oneCommandShouldGenerateEventAndPersistProjection() {

        //Set up
        InMemoryEventStore<Tuple0, VikingEvent, Tuple0, Tuple0> inMemoryEventStore = InMemoryEventStore.create(actorSystem);
        VikingProjection projection = new VikingProjection();
        EventProcessor<String, Viking, VikingCommand, VikingEvent, Tuple0, String, Tuple0, Tuple0> vikingEventProcessor = vikingEventProcessorWithProjection(inMemoryEventStore, List.of(projection));

        //Test
        Either<String, ProcessingSuccess<Viking, VikingEvent, Tuple0, Tuple0, String>> result = vikingEventProcessor.processCommand(new CreateViking("1", "ragnar")).get();

        // Results
        assertThat(result.isRight()).isTrue();
        ProcessingSuccess<Viking, VikingEvent, Tuple0, Tuple0, String> eventAndState = result.get();
        assertThat(eventAndState.getPreviousState()).isEqualTo(Option.none());
        Viking expected = new Viking("1", "ragnar", 1L);
        assertThat(eventAndState.getCurrentState()).isEqualTo(Option.some(expected));

        java.util.List<EventEnvelope<VikingEvent, Tuple0, Tuple0>> eventsFromJournal = inMemoryEventStore.loadAllEvents().runWith(Sink.seq(), Materializer.createMaterializer(actorSystem)).toCompletableFuture().join();


        EventEnvelope<VikingEvent, Tuple0, Tuple0> expectedEnvelope = EventEnvelope.<VikingEvent, Tuple0, Tuple0>builder()
                .withEmissionDate(eventsFromJournal.get(0).emissionDate)
                .withId(eventsFromJournal.get(0).id)
                .withEntityId("1")
                .withSequenceNum(1L)
                .withEventType(VikingEvent.VikingCreatedV1.name())
                .withVersion(VikingEvent.VikingCreatedV1.version())
                .withTotalMessageInTransaction(1)
                .withNumMessageInTransaction(1)
                .withTransactionId("1")
                .withEvent(new VikingEvent.VikingCreated("1", "ragnar"))
                .build();
        assertThat(eventAndState.getEvents()).containsExactly(expectedEnvelope);
        assertThat(eventAndState.getMessage()).isEqualTo("C");

        assertThat(eventsFromJournal).containsExactly(expectedEnvelope);

        assertThat(vikingEventProcessor.getAggregateStore().getAggregate(Tuple.empty(), "1").get()).isEqualTo(Some(expected));
        assertThat(projection.data.get("1")).isEqualTo(1);
    }


    @Test
    public void twoCommandShouldGenerateEventAndPersistState() {
        VikingSnapshot vikingSnapshot = new VikingSnapshot();
        InMemoryEventStore<Tuple0, VikingEvent, Tuple0, Tuple0> inMemoryEventStore = InMemoryEventStore.create(actorSystem);
        EventProcessor<String, Viking, VikingCommand, VikingEvent, Tuple0, String, Tuple0, Tuple0> vikingEventProcessor = vikingEventProcessor(inMemoryEventStore, vikingSnapshot);

        vikingEventProcessor.processCommand(new VikingCommand.CreateViking("1", "ragnar")).get();
        Either<String, ProcessingSuccess<Viking, VikingEvent, Tuple0, Tuple0, String>> result = vikingEventProcessor.processCommand(new UpdateViking("1", "Ragnar Lodbrock")).get();

        assertThat(result.isRight()).isTrue();
        ProcessingSuccess<Viking, VikingEvent, Tuple0, Tuple0, String> eventAndState = result.get();
        Viking intermediateState = new Viking("1", "ragnar", 1L);

        assertThat(eventAndState.getPreviousState()).isEqualTo(Some(intermediateState));

        Viking expected = new Viking("1", "Ragnar Lodbrock", 2L);
        assertThat(eventAndState.getCurrentState()).isEqualTo(Option.some(expected));

        java.util.List<EventEnvelope<VikingEvent, Tuple0, Tuple0>> eventsFromJournal = inMemoryEventStore.loadAllEvents().runWith(Sink.seq(), Materializer.createMaterializer(actorSystem)).toCompletableFuture().join();

        EventEnvelope<VikingEvent, Tuple0, Tuple0> eventEnvelope1 = EventEnvelope.<VikingEvent, Tuple0, Tuple0>builder()
                .withEmissionDate(eventsFromJournal.get(0).emissionDate)
                .withId(eventsFromJournal.get(0).id)
                .withEntityId("1")
                .withSequenceNum(1L)
                .withEventType(VikingEvent.VikingCreatedV1.name())
                .withVersion(VikingEvent.VikingCreatedV1.version())
                .withTotalMessageInTransaction(1)
                .withNumMessageInTransaction(1)
                .withTransactionId("1")
                .withEvent(new VikingEvent.VikingCreated("1", "ragnar"))
                .build();

        EventEnvelope<VikingEvent, Tuple0, Tuple0> eventEnvelope2 = EventEnvelope.<VikingEvent, Tuple0, Tuple0>builder()
                .withEmissionDate(eventsFromJournal.get(1).emissionDate)
                .withId(eventsFromJournal.get(1).id)
                .withEntityId("1")
                .withSequenceNum(2L)
                .withEventType(VikingEvent.VikingUpdatedV1.name())
                .withVersion(VikingEvent.VikingUpdatedV1.version())
                .withTotalMessageInTransaction(1)
                .withNumMessageInTransaction(1)
                .withTransactionId("2")
                .withEvent(new VikingEvent.VikingUpdated("1", "Ragnar Lodbrock"))
                .build();

        assertThat(eventAndState.getEvents()).containsExactly(eventEnvelope2);

        assertThat(eventsFromJournal).containsExactly(eventEnvelope1, eventEnvelope2);

        assertThat(vikingSnapshot.data.get("1")).isEqualTo(expected);
    }

    @Test
    public void createAndDeleteShouldGenerateEventAndPersistState() {
        VikingSnapshot vikingSnapshot = new VikingSnapshot();
        InMemoryEventStore<Tuple0, VikingEvent, Tuple0, Tuple0> inMemoryEventStore = InMemoryEventStore.create(actorSystem);
        EventProcessor<String, Viking, VikingCommand, VikingEvent, Tuple0, String, Tuple0, Tuple0> vikingEventProcessor = vikingEventProcessor(inMemoryEventStore, vikingSnapshot);

        vikingEventProcessor.processCommand(new VikingCommand.CreateViking("1", "ragnar")).get();
        Either<String, ProcessingSuccess<Viking, VikingEvent, Tuple0, Tuple0, String>> result = vikingEventProcessor.processCommand(new DeleteViking("1")).get();

        assertThat(result.isRight()).isTrue();
        ProcessingSuccess<Viking, VikingEvent, Tuple0, Tuple0, String> eventAndState = result.get();
        Viking intermediateState = new Viking("1", "ragnar", 1L);

        assertThat(eventAndState.getPreviousState()).isEqualTo(Some(intermediateState));

        assertThat(eventAndState.getCurrentState()).isEqualTo(Option.none());

        java.util.List<EventEnvelope<VikingEvent, Tuple0, Tuple0>> eventsFromJournal = inMemoryEventStore.loadAllEvents().runWith(Sink.seq(), Materializer.createMaterializer(actorSystem)).toCompletableFuture().join();

        EventEnvelope<VikingEvent, Tuple0, Tuple0> eventEnvelope1 = EventEnvelope.<VikingEvent, Tuple0, Tuple0>builder()
                .withEmissionDate(eventsFromJournal.get(0).emissionDate)
                .withId(eventsFromJournal.get(0).id)
                .withEntityId("1")
                .withSequenceNum(1L)
                .withEventType(VikingEvent.VikingCreatedV1.name())
                .withVersion(VikingEvent.VikingCreatedV1.version())
                .withTotalMessageInTransaction(1)
                .withNumMessageInTransaction(1)
                .withTransactionId("1")
                .withEvent(new VikingEvent.VikingCreated("1", "ragnar"))
                .build();

        EventEnvelope<VikingEvent, Tuple0, Tuple0> eventEnvelope2 = EventEnvelope.<VikingEvent, Tuple0, Tuple0>builder()
                .withEmissionDate(eventsFromJournal.get(1).emissionDate)
                .withId(eventAndState.getEvents().get(0).id)
                .withEntityId("1")
                .withSequenceNum(2L)
                .withEventType(VikingEvent.VikingDeletedV1.name())
                .withVersion(VikingEvent.VikingDeletedV1.version())
                .withTotalMessageInTransaction(1)
                .withNumMessageInTransaction(1)
                .withTransactionId("2")
                .withEvent(new VikingEvent.VikingDeleted("1"))
                .build();

        assertThat(eventAndState.getEvents()).containsExactly(eventEnvelope2);

        assertThat(eventsFromJournal).containsExactly(eventEnvelope1, eventEnvelope2);

        assertThat(vikingSnapshot.data.get("1")).isNull();
    }


    private EventProcessor<String, Viking, VikingCommand, VikingEvent, Tuple0, String, Tuple0, Tuple0> vikingEventProcessor(InMemoryEventStore<Tuple0, VikingEvent, Tuple0, Tuple0> inMemoryEventStore, VikingSnapshot vikingSnapshot) {
        return new EventProcessor<>(
                inMemoryEventStore,
                new FakeTransactionManager(),
                vikingSnapshot,
                new VikingCommandHandler(),
                new VikingEventHandler()
        );
    }

    private EventProcessor<String, Viking, VikingCommand, VikingEvent, Tuple0, String, Tuple0, Tuple0> vikingEventProcessorWithProjection(InMemoryEventStore<Tuple0, VikingEvent, Tuple0, Tuple0> inMemoryEventStore, List<Projection<Tuple0, VikingEvent, Tuple0, Tuple0>> projections) {
        return new EventProcessor<>(
                actorSystem,
                inMemoryEventStore,
                new FakeTransactionManager(),
                new VikingCommandHandler(),
                new VikingEventHandler(),
                projections
        );
    }


    public static class FakeTransactionManager implements TransactionManager<Tuple0> {

        private AtomicInteger counter = new AtomicInteger(0);

        @Override
        public <T> Future<T> withTransaction(Function<Tuple0, Future<T>> callBack) {
            return callBack.apply(Tuple.empty());
        }

        @Override
        public String transactionId() {
            return String.valueOf(counter.incrementAndGet());
        }
    }


}