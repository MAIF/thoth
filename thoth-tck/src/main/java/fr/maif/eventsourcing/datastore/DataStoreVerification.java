package fr.maif.eventsourcing.datastore;

import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.eventsourcing.EventProcessor;
import fr.maif.eventsourcing.ProcessingSuccess;
import io.vavr.Tuple0;
import io.vavr.control.Either;
import io.vavr.control.Option;
import org.testng.annotations.Test;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class DataStoreVerification<TxCtx> implements DataStoreVerificationRules<TestState, TestEvent, Tuple0, Tuple0, TxCtx>{

    public abstract EventProcessor<String, TestState, TestCommand, TestEvent, TxCtx, Tuple0, Tuple0, Tuple0> eventProcessor(String topic);
    public abstract String kafkaBootstrapUrl();

    @Override
    @Test
    public void required_submitValidSingleEventCommandMustWriteEventInDataStore() {
        String topic = randomKafkaTopic();
        final EventProcessor<String, TestState, TestCommand, TestEvent, TxCtx, Tuple0, Tuple0, Tuple0> eventProcessor = eventProcessor(topic);
        submitValidCommand(eventProcessor, "1");

        List<EventEnvelope<TestEvent, Tuple0, Tuple0>> envelopes = deduplicateOnId(readFromDataStore(eventProcessor.eventStore()));

        cleanup(eventProcessor);
        assertThat(envelopes).hasSize(1);
    }

    @Override
    @Test
    public void required_submitInvalidCommandMustNotWriteEventsIntDataStore() {
        String topic = randomKafkaTopic();
        final EventProcessor<String, TestState, TestCommand, TestEvent, TxCtx, Tuple0, Tuple0, Tuple0> eventProcessor = eventProcessor(topic);
        submitInvalidCommand(eventProcessor, "1");

        List<EventEnvelope<TestEvent, Tuple0, Tuple0>> envelopes = deduplicateOnId(readFromDataStore(eventProcessor.eventStore()));

        cleanup(eventProcessor);
        assertThat(envelopes).isEmpty();
    }

    @Override
    @Test
    public void required_submitMultiEventCommandMustWriteAllEventsInDataStore() {
        String topic = randomKafkaTopic();
        final EventProcessor<String, TestState, TestCommand, TestEvent, TxCtx, Tuple0, Tuple0, Tuple0> eventProcessor = eventProcessor(topic);
        submitMultiEventsCommand(eventProcessor, "1");

        List<EventEnvelope<TestEvent, Tuple0, Tuple0>> envelopes = deduplicateOnId(readFromDataStore(eventProcessor.eventStore()));

        cleanup(eventProcessor);

        assertThat(envelopes.size()).isGreaterThan(1);
        List<UUID> ids = envelopes.stream().map(envelope -> envelope.id).collect(Collectors.toList());

        assertThat(ids).doesNotHaveDuplicates();
    }

    @Override
    @Test
    public void required_aggregateOfSingleEventStateShouldBeCorrect() {
        String topic = randomKafkaTopic();
        EventProcessor<String, TestState, TestCommand, TestEvent, TxCtx, Tuple0, Tuple0, Tuple0> eventProcessor = eventProcessor(topic);

        submitValidCommand(eventProcessor, "1");
        Option<TestState> state = readState(eventProcessor, "1");

        cleanup(eventProcessor);

        assertThat(state.isDefined()).isTrue();
        assertThat(state.get().count).isEqualTo(1);
    }

    @Override
    @Test
    public void required_aggregateOfDeleteEventStateShouldBeEmpty() {
        String topic = randomKafkaTopic();
        EventProcessor<String, TestState, TestCommand, TestEvent, TxCtx, Tuple0, Tuple0, Tuple0> eventProcessor = eventProcessor(topic);

        submitValidCommand(eventProcessor, "1");
        submitDeleteCommand(eventProcessor, "1");
        Option<TestState> state = readState(eventProcessor, "1");

        cleanup(eventProcessor);

        assertThat(state.isEmpty()).isTrue();
    }

    @Override
    @Test
    public void required_aggregateOfMultipleEventStateShouldBeCorrect() {
        String topic = randomKafkaTopic();
        EventProcessor<String, TestState, TestCommand, TestEvent, TxCtx, Tuple0, Tuple0, Tuple0> eventProcessor = eventProcessor(topic);

        submitMultiEventsCommand(eventProcessor, "1");
        Option<TestState> state = readState(eventProcessor, "1");

        cleanup(eventProcessor);

        assertThat(state.isDefined()).isTrue();
        assertThat(state.get().count).isEqualTo(2);
    }

    @Override
    @Test
    public void required_singleEventShouldBePublished() {
        String topic = randomKafkaTopic();
        EventProcessor<String, TestState, TestCommand, TestEvent, TxCtx, Tuple0, Tuple0, Tuple0> eventProcessor = eventProcessor(topic);

        submitValidCommand(eventProcessor, "1");
        List<EventEnvelope<TestEvent, Tuple0, Tuple0>> envelopes = deduplicateOnId(readPublishedEvents(kafkaBootstrapUrl(), topic));

        cleanup(eventProcessor);

        assertThat(envelopes).hasSize(1);
    }

    @Override
    @Test
    public void required_multipleEventsShouldBePublished() {
        String topic = randomKafkaTopic();
        EventProcessor<String, TestState, TestCommand, TestEvent, TxCtx, Tuple0, Tuple0, Tuple0> eventProcessor = eventProcessor(topic);
        submitMultiEventsCommand(eventProcessor, "1");
        List<EventEnvelope<TestEvent, Tuple0, Tuple0>> envelopes = deduplicateOnId(readPublishedEvents(kafkaBootstrapUrl(), topic));

        cleanup(eventProcessor);

        assertThat(envelopes).hasSize(2);
    }

    @Override
    @Test
    public void required_eventShouldBePublishedEventIfBrokerIsDownAtFirst() {
        String topic = randomKafkaTopic();
        EventProcessor<String, TestState, TestCommand, TestEvent, TxCtx, Tuple0, Tuple0, Tuple0> eventProcessor = eventProcessor(topic);
        shutdownBroker();
        submitValidCommand(eventProcessor, "1");

        restartBroker();
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        List<EventEnvelope<TestEvent, Tuple0, Tuple0>> envelopes = deduplicateOnId(readPublishedEvents(kafkaBootstrapUrl(), topic));

        cleanup(eventProcessor);

        assertThat(envelopes).hasSize(1);
    }

    @Override
    @Test
    public void required_commandSubmissionShouldFailIfDatabaseIsNotAvailable() {
        String topic = randomKafkaTopic();
        EventProcessor<String, TestState, TestCommand, TestEvent, TxCtx, Tuple0, Tuple0, Tuple0> eventProcessor = eventProcessor(topic);
        shutdownDatabase();
        try {
            Either<String, ProcessingSuccess<TestState, TestEvent, Tuple0, Tuple0, Tuple0>> result = submitValidCommand(eventProcessor, "1");

            cleanup(eventProcessor);

            assertThat(result.isLeft()).isTrue();
        } catch(Throwable ex) {
            // implementation should either return an embedded error in either, either throw an exception
        }finally {
            restartDatabase();
        }
    }

    @Override
    public Either<String, ProcessingSuccess<TestState, TestEvent, Tuple0, Tuple0, Tuple0>> submitValidCommand(
            EventProcessor<String, TestState, TestCommand, TestEvent, TxCtx, Tuple0, Tuple0, Tuple0> eventProcessor,
            String id) {
        return eventProcessor.processCommand(new TestCommand.SimpleCommand(id)).toCompletableFuture().join();
    }

    @Override
    public void submitInvalidCommand(
            EventProcessor<String, TestState, TestCommand, TestEvent, TxCtx, Tuple0, Tuple0, Tuple0> eventProcessor,
            String id
    ) {
        eventProcessor.processCommand(new TestCommand.InvalidCommand(id)).toCompletableFuture().join();

    }

    @Override
    public void submitMultiEventsCommand(
            EventProcessor<String, TestState, TestCommand, TestEvent, TxCtx, Tuple0, Tuple0, Tuple0> eventProcessor,
            String id
    ) {
        eventProcessor.processCommand(new TestCommand.MultiEventCommand(id)).toCompletableFuture().join();
    }

    @Override
    public void submitDeleteCommand(EventProcessor<String, TestState, TestCommand, TestEvent, TxCtx, Tuple0, Tuple0, Tuple0> eventProcessor, String id) {
        eventProcessor.processCommand(new TestCommand.DeleteCommand(id)).toCompletableFuture().join();
    }

    @Override
    public Option<TestState> readState(EventProcessor<String, TestState, TestCommand, TestEvent, TxCtx, Tuple0, Tuple0, Tuple0> eventProcessor, String id) {
        return eventProcessor.getAggregate(id).toCompletableFuture().join();
    }

//    @Override
//    public List<EventEnvelope<TestEvent, Tuple0, Tuple0>> readFromDataStore(EventStore<TxCtx, TestEvent, Tuple0, Tuple0> eventStore) {
//        try {
//            return eventStore.loadAllEvents().runWith(Sink.seq(), actorSystem).toCompletableFuture().get();
//        } catch (InterruptedException | ExecutionException e) {
//            throw new RuntimeException(e);
//        }
//    }

    public String randomKafkaTopic() {
        return "test-topic" + UUID.randomUUID();
    }

    private List<EventEnvelope<TestEvent, Tuple0, Tuple0>> deduplicateOnId(List<EventEnvelope<TestEvent, Tuple0, Tuple0>> envelopes) {
        return io.vavr.collection.List.ofAll(envelopes).distinctBy(env -> env.id).toJavaList();
    }
}
