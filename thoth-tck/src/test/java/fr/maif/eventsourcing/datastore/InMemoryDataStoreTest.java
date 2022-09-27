package fr.maif.eventsourcing.datastore;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import akka.stream.javadsl.Source;
import fr.maif.akka.eventsourcing.DefaultAggregateStore;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;

import akka.actor.ActorSystem;
import akka.stream.javadsl.Sink;
import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.eventsourcing.EventProcessorImpl;
import fr.maif.eventsourcing.EventStore;
import fr.maif.eventsourcing.TransactionManager;
import fr.maif.akka.eventsourcing.InMemoryEventStore;
import io.vavr.Tuple;
import io.vavr.Tuple0;

public class InMemoryDataStoreTest extends DataStoreVerification<Tuple0> {
    public ActorSystem actorSystem = ActorSystem.create();
    public InMemoryEventStore<TestEvent, Tuple0, Tuple0> eventStore;
    public EventProcessorImpl<String, TestState, TestCommand, TestEvent, Tuple0, Tuple0, Tuple0, Tuple0> eventProcessor;

    @BeforeMethod(alwaysRun = true)
    public void init() {
        this.eventStore = Mockito.spy(InMemoryEventStore.create(actorSystem));
        TestEventHandler eventHandler = new TestEventHandler();
        TransactionManager<Tuple0> transactionManager = noOpTransactionManager();
        this.eventProcessor = new EventProcessorImpl<>(
                eventStore,
                transactionManager,
                new DefaultAggregateStore<>(eventStore, eventHandler, actorSystem, transactionManager),
                new TestCommandHandler<>(),
                eventHandler,
                io.vavr.collection.List.empty()
        );
    }

    @Override
    public List<EventEnvelope<TestEvent, Tuple0, Tuple0>> readPublishedEvents(String kafkaBootStrapUrl, String topic) {
        try {
            return Source.fromPublisher(this.eventStore.loadAllEvents()).runWith(Sink.seq(), actorSystem).toCompletableFuture().get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void required_commandSubmissionShouldFailIfDatabaseIsNotAvailable() {
        // Not implemented for in memory
    }


    @Override
    public void required_eventShouldBePublishedEventIfBrokerIsDownAtFirst() {
        // Not implemented for in memory
    }

    @Override
    public void shutdownBroker() {
        throw new RuntimeException("Not implemented for in memory");
    }

    @Override
    public void restartBroker() {
        Mockito.reset(eventStore);
        throw new RuntimeException("Not implemented for in memory");
    }

    @Override
    public void shutdownDatabase() {
        throw new RuntimeException("Not implemented for in memory");
    }

    @Override
    public void restartDatabase() {
        throw new RuntimeException("Not implemented for in memory");
    }

    @Override
    public EventProcessorImpl<String, TestState, TestCommand, TestEvent, Tuple0, Tuple0, Tuple0, Tuple0> eventProcessor(String topic) {
        return this.eventProcessor;
    }

    @Override
    public List<EventEnvelope<TestEvent, Tuple0, Tuple0>> readFromDataStore(EventStore<Tuple0, TestEvent, Tuple0, Tuple0> eventStore) {
        try {
            return Source.fromPublisher(eventStore.loadAllEvents()).runWith(Sink.seq(), actorSystem).toCompletableFuture().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String kafkaBootstrapUrl() {
        return null;
    }

    private TransactionManager<Tuple0> noOpTransactionManager() {
        return new TransactionManager<Tuple0>() {
            @Override
            public <T> CompletionStage<T> withTransaction(Function<Tuple0, CompletionStage<T>> function) {
                return function.apply(Tuple.empty());
            }
        };
    }
}
