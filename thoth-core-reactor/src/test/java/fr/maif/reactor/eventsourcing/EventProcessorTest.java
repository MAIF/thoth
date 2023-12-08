package fr.maif.reactor.eventsourcing;

import fr.maif.eventsourcing.*;
import fr.maif.reactor.eventsourcing.InMemoryEventStore.Transaction;
import io.vavr.API;
import io.vavr.Tuple;
import io.vavr.Tuple0;
import io.vavr.collection.List;
import io.vavr.control.Either;
import io.vavr.control.Option;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static fr.maif.Helpers.*;
import static fr.maif.Helpers.VikingCommand.*;
import static io.vavr.API.Some;
import static org.assertj.core.api.Assertions.anyOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

public class EventProcessorTest {


    @Test
    public void oneCommandShouldGenerateEventAndPersistState() {

        //Set up
        VikingSnapshot vikingSnapshot = new VikingSnapshot();
        InMemoryEventStore<VikingEvent, Tuple0, Tuple0> inMemoryEventStore = InMemoryEventStore.create();
        var vikingEventProcessor = vikingEventProcessor(inMemoryEventStore, vikingSnapshot);

        //Test
        Either<String, ProcessingSuccess<Viking, VikingEvent, Tuple0, Tuple0, String>> result = vikingEventProcessor.processCommand(new CreateViking("1", "ragnar", 30)).toCompletableFuture().join();

        // Results
        assertThat(result.isRight()).isTrue();
        ProcessingSuccess<Viking, VikingEvent, Tuple0, Tuple0, String> eventAndState = result.get();
        Assertions.assertThat(eventAndState.getPreviousState()).isEqualTo(Option.none());
        Viking expected = new Viking("1", "ragnar", 30, 1L);
        Assertions.assertThat(eventAndState.getCurrentState()).isEqualTo(Option.some(expected));

        java.util.List<EventEnvelope<VikingEvent, Tuple0, Tuple0>> eventsFromJournal = Flux.from(inMemoryEventStore.loadAllEvents()).collectList().block();

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
                .withEvent(new VikingEvent.VikingCreated("1", "ragnar", 30))
                .build();
        assertThat(eventAndState.getEvents()).containsExactly(expectedEnvelope);
        assertThat(eventAndState.getMessage()).isEqualTo("C");

        assertThat(eventsFromJournal).containsExactly(expectedEnvelope);

        Assertions.assertThat(vikingSnapshot.data.get("1")).isEqualTo(expected);
    }


    @Test
    public void oneCommandShouldGenerateEventAndPersistProjection() {

        //Set up
        InMemoryEventStore<VikingEvent, Tuple0, Tuple0> inMemoryEventStore = InMemoryEventStore.create();
        VikingProjection projection = new VikingProjection();
        var vikingEventProcessor = vikingEventProcessorWithProjection(inMemoryEventStore, List.of(projection));

        //Test
        Either<String, ProcessingSuccess<Viking, VikingEvent, Tuple0, Tuple0, String>> result = vikingEventProcessor.processCommand(new CreateViking("1", "ragnar", 30)).toCompletableFuture().join();

        // Results
        assertThat(result.isRight()).isTrue();
        ProcessingSuccess<Viking, VikingEvent, Tuple0, Tuple0, String> eventAndState = result.get();
        Assertions.assertThat(eventAndState.getPreviousState()).isEqualTo(Option.none());
        Viking expected = new Viking("1", "ragnar", 30, 1L);
        Assertions.assertThat(eventAndState.getCurrentState()).isEqualTo(Option.some(expected));

        java.util.List<EventEnvelope<VikingEvent, Tuple0, Tuple0>> eventsFromJournal = Flux.from(inMemoryEventStore.loadAllEvents()).collectList().block();


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
                .withEvent(new VikingEvent.VikingCreated("1", "ragnar", 30))
                .build();
        assertThat(eventAndState.getEvents()).containsExactly(expectedEnvelope);
        assertThat(eventAndState.getMessage()).isEqualTo("C");

        assertThat(eventsFromJournal).containsExactly(expectedEnvelope);

        assertThat(vikingEventProcessor.getAggregateStore().getAggregate(any(), eq("1")).toCompletableFuture().join()).isEqualTo(Some(expected));
        Assertions.assertThat(projection.data.get("1")).isEqualTo(1);
    }


    @Test
    public void twoCommandShouldGenerateEventAndPersistState() {
        VikingSnapshot vikingSnapshot = new VikingSnapshot();
        InMemoryEventStore<VikingEvent, Tuple0, Tuple0> inMemoryEventStore = InMemoryEventStore.create();
        var vikingEventProcessor = vikingEventProcessor(inMemoryEventStore, vikingSnapshot);

        vikingEventProcessor.processCommand(new VikingCommand.CreateViking("1", "ragnar", 30)).toCompletableFuture().join();
        Either<String, ProcessingSuccess<Viking, VikingEvent, Tuple0, Tuple0, String>> result = vikingEventProcessor.processCommand(new UpdateViking("1", "Ragnar Lodbrock", 30)).toCompletableFuture().join();

        assertThat(result.isRight()).isTrue();
        ProcessingSuccess<Viking, VikingEvent, Tuple0, Tuple0, String> eventAndState = result.get();
        Viking intermediateState = new Viking("1", "ragnar", 30, 1L);

        Assertions.assertThat(eventAndState.getPreviousState()).isEqualTo(Some(intermediateState));

        Viking expected = new Viking("1", "Ragnar Lodbrock", 30, 2L);
        Assertions.assertThat(eventAndState.getCurrentState()).isEqualTo(Option.some(expected));

        java.util.List<EventEnvelope<VikingEvent, Tuple0, Tuple0>> eventsFromJournal = Flux.from(inMemoryEventStore.loadAllEvents()).collectList().block();

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
                .withEvent(new VikingEvent.VikingCreated("1", "ragnar", 30))
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
                .withEvent(new VikingEvent.VikingUpdated("1", "Ragnar Lodbrock", 30))
                .build();

        assertThat(eventAndState.getEvents()).containsExactly(eventEnvelope2);

        assertThat(eventsFromJournal).containsExactly(eventEnvelope1, eventEnvelope2);

        Assertions.assertThat(vikingSnapshot.data.get("1")).isEqualTo(expected);
    }

    @Test
    public void createAndDeleteShouldGenerateEventAndPersistState() {
        VikingSnapshot vikingSnapshot = new VikingSnapshot();
        InMemoryEventStore<VikingEvent, Tuple0, Tuple0> inMemoryEventStore = InMemoryEventStore.create();
        var vikingEventProcessor = vikingEventProcessor(inMemoryEventStore, vikingSnapshot);

        vikingEventProcessor.processCommand(new VikingCommand.CreateViking("1", "ragnar", 30)).toCompletableFuture().join();
        Either<String, ProcessingSuccess<Viking, VikingEvent, Tuple0, Tuple0, String>> result = vikingEventProcessor.processCommand(new DeleteViking("1")).toCompletableFuture().join();

        assertThat(result.isRight()).isTrue();
        ProcessingSuccess<Viking, VikingEvent, Tuple0, Tuple0, String> eventAndState = result.get();
        Viking intermediateState = new Viking("1", "ragnar", 30, 1L);

        Assertions.assertThat(eventAndState.getPreviousState()).isEqualTo(Some(intermediateState));

        Assertions.assertThat(eventAndState.getCurrentState()).isEqualTo(Option.none());

        java.util.List<EventEnvelope<VikingEvent, Tuple0, Tuple0>> eventsFromJournal = Flux.from(inMemoryEventStore.loadAllEvents()).collectList().block();

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
                .withEvent(new VikingEvent.VikingCreated("1", "ragnar", 30))
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

        Assertions.assertThat(vikingSnapshot.data.get("1")).isNull();
    }

    @Test
    public void multipleCommandsOnSameEntityOnSameBatch() {
        VikingSnapshot vikingSnapshot = new VikingSnapshot();
        InMemoryEventStore<VikingEvent, Tuple0, Tuple0> inMemoryEventStore = InMemoryEventStore.create();
        var vikingEventProcessor = vikingEventProcessor(inMemoryEventStore, vikingSnapshot);

        var result = vikingEventProcessor.batchProcessCommand(API.List(
                new VikingCommand.CreateViking("1", "ragnar", 30),
                new VikingCommand.UpdateViking("1", "Ragnar Lodbrock", 30),
                new VikingCommand.UpdateAge("1", 35)
        )).toCompletableFuture().join();

        Option<Viking> mayBeState = vikingEventProcessor.getAggregate("1").toCompletableFuture().join();

        assertThat(mayBeState).isEqualTo(Some(new Viking("1", "Ragnar Lodbrock", 35, 3L)));
        List<EventEnvelope<VikingEvent, Tuple0, Tuple0>> eventsFromJournal = List.ofAll(Flux.from(inMemoryEventStore.loadAllEvents()).collectList().block());
        List<VikingEvent> events = eventsFromJournal.map(evt -> evt.event());
        assertThat(events).containsExactly(
                new VikingEvent.VikingCreated("1", "ragnar", 30),
                new VikingEvent.VikingUpdated("1", "Ragnar Lodbrock", 30),
                new VikingEvent.VikingUpdated("1", "Ragnar Lodbrock", 35)
        );
    }


    private EventProcessorImpl<String, Viking, VikingCommand, VikingEvent, Transaction<VikingEvent, Tuple0, Tuple0>, String, Tuple0, Tuple0> vikingEventProcessor(InMemoryEventStore<VikingEvent, Tuple0, Tuple0> inMemoryEventStore, VikingSnapshot vikingSnapshot) {
        return new EventProcessorImpl<>(
                inMemoryEventStore,
                new FakeTransactionManager(),
                vikingSnapshot,
                new VikingCommandHandler(),
                new VikingEventHandler(),
                List.empty()
        );
    }


    private EventProcessorImpl<String, Viking, VikingCommand, VikingEvent, Transaction<VikingEvent, Tuple0, Tuple0>, String, Tuple0, Tuple0> vikingEventProcessor(CommandHandler<String, Viking, VikingCommand, VikingEvent, String, Transaction<VikingEvent, Tuple0, Tuple0>> commandHandler, InMemoryEventStore<VikingEvent, Tuple0, Tuple0> inMemoryEventStore, VikingSnapshot vikingSnapshot) {
        return new EventProcessorImpl<>(
                inMemoryEventStore,
                new FakeTransactionManager(),
                vikingSnapshot,
                commandHandler,
                new VikingEventHandler(),
                List.empty()
        );
    }

    private EventProcessorImpl<String, Viking, VikingCommand, VikingEvent, Transaction<VikingEvent, Tuple0, Tuple0>, String, Tuple0, Tuple0> vikingEventProcessorWithProjection(InMemoryEventStore<VikingEvent, Tuple0, Tuple0> inMemoryEventStore, List<Projection<Transaction<VikingEvent, Tuple0, Tuple0>, VikingEvent, Tuple0, Tuple0>> projections) {
        VikingEventHandler vikingEventHandler = new VikingEventHandler();
        FakeTransactionManager fakeTransactionManager = new FakeTransactionManager();
        return new EventProcessorImpl<>(
                inMemoryEventStore,
                fakeTransactionManager,
                new DefaultAggregateStore<>(inMemoryEventStore, vikingEventHandler, fakeTransactionManager),
                new VikingCommandHandler(),
                vikingEventHandler,
                projections
        );
    }


    private EventProcessorImpl<String, Viking, VikingCommand, VikingEvent, Transaction<VikingEvent, Tuple0, Tuple0>, String, Tuple0, Tuple0> vikingEventProcessorWithSnapshot(InMemoryEventStore<VikingEvent, Tuple0, Tuple0> inMemoryEventStore, AggregateStore<Viking, String, Transaction<VikingEvent, Tuple0, Tuple0>> aggregateStore, List<Projection<Transaction<VikingEvent, Tuple0, Tuple0>, VikingEvent, Tuple0, Tuple0>> projections) {
        return new EventProcessorImpl<>(
                inMemoryEventStore,
                new FakeTransactionManager(),
                aggregateStore,
                new VikingCommandHandler(),
                new VikingEventHandler(),
                projections
        );
    }


    public static class FakeTransactionManager implements TransactionManager<Transaction<VikingEvent, Tuple0, Tuple0>> {

        private AtomicInteger counter = new AtomicInteger(0);


        @Override
        public <T> CompletionStage<T> withTransaction(Function<Transaction<VikingEvent, Tuple0, Tuple0>, CompletionStage<T>> callBack) {
            return callBack.apply(new Transaction<>());
        }

        @Override
        public String transactionId() {
            return String.valueOf(counter.incrementAndGet());
        }
    }


}