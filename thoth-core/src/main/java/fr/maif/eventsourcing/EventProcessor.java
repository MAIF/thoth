package fr.maif.eventsourcing;

import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.impl.TimeBasedGenerator;
import fr.maif.concurrent.CompletionStages;
import fr.maif.eventsourcing.TransactionManager.InTransactionResult;
import io.vavr.Tuple;
import io.vavr.Tuple3;
import io.vavr.Value;
import io.vavr.collection.List;
import io.vavr.collection.Seq;
import io.vavr.control.Either;
import io.vavr.control.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

import static fr.maif.concurrent.CompletionStages.traverse;
import static io.vavr.API.*;

public class EventProcessor<Error, S extends State<S>, C extends Command<Meta, Context>, E extends Event, TxCtx, Message, Meta, Context> {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventProcessor.class);
    private static final TimeBasedGenerator UUIDgen = Generators.timeBasedGenerator();

    private final EventStore<TxCtx, E, Meta, Context> eventStore;
    private final TransactionManager<TxCtx> transactionManager;

    private final AggregateStore<S, String, TxCtx> aggregateStore;
    private final CommandHandler<Error, S, C, E, Message, TxCtx> commandHandler;
    private final EventHandler<S, E> eventHandler;
    private final List<Projection<TxCtx, E, Meta, Context>> projections;

//    public static <Error, S extends State<S>, C extends Command<Meta, Context>, E extends Event, TxCtx, Message, Meta, Context> EventProcessor<Error, S, C, E, TxCtx, Message, Meta, Context> create(ActorSystem system, EventStore<TxCtx, E, Meta, Context> eventStore, TransactionManager<TxCtx> transactionManager, CommandHandler<Error, S, C, E, Message, TxCtx> commandHandler, EventHandler<S, E> eventHandler, List<Projection<TxCtx, E, Meta, Context>> projections) {
//        return new EventProcessor<>(eventStore, transactionManager, new DefaultAggregateStore<>(eventStore, eventHandler, system, transactionManager), commandHandler, eventHandler, projections);
//    }
//
//    public EventProcessor(ActorSystem system, EventStore<TxCtx, E, Meta, Context> eventStore, TransactionManager<TxCtx> transactionManager, CommandHandler<Error, S, C, E, Message, TxCtx> commandHandler, EventHandler<S, E> eventHandler, List<Projection<TxCtx, E, Meta, Context>> projections) {
//        this(eventStore, transactionManager, new DefaultAggregateStore<>(eventStore, eventHandler, system, transactionManager), commandHandler, eventHandler, projections);
//    }

    public EventProcessor(EventStore<TxCtx, E, Meta, Context> eventStore, TransactionManager<TxCtx> transactionManager, AggregateStore<S, String, TxCtx> aggregateStore, CommandHandler<Error, S, C, E, Message, TxCtx> commandHandler, EventHandler<S, E> eventHandler, List<Projection<TxCtx, E, Meta, Context>> projections) {
        this.eventStore = eventStore;
        this.transactionManager = transactionManager;
        this.aggregateStore = aggregateStore;
        this.commandHandler = commandHandler;
        this.eventHandler = eventHandler;
        this.projections = projections;
    }

    public EventProcessor(
            EventStore<TxCtx, E, Meta, Context> eventStore,
            TransactionManager<TxCtx> transactionManager,
            SnapshotStore<S, String, TxCtx> aggregateStore,
            CommandHandler<Error, S, C, E, Message, TxCtx> commandHandler,
            EventHandler<S, E> eventHandler) {

        this.projections = List.empty();
        this.eventStore = eventStore;
        this.transactionManager = transactionManager;
        this.aggregateStore = aggregateStore;
        this.commandHandler = commandHandler;
        this.eventHandler = eventHandler;
    }

    public CompletionStage<Either<Error, ProcessingSuccess<S, E, Meta, Context, Message>>> processCommand(C command) {
        LOGGER.debug("Processing command {}", command);
        return batchProcessCommand(List(command)).thenApply(List::head);
    }


    public CompletionStage<List<Either<Error, ProcessingSuccess<S, E, Meta, Context, Message>>>> batchProcessCommand(List<C> commands) {
        LOGGER.debug("Processing commands {}", commands);
        return transactionManager
                .withTransaction(ctx -> batchProcessCommand(ctx, commands))
                .thenCompose(InTransactionResult::postTransaction);
    }

    public CompletionStage<InTransactionResult<List<Either<Error, ProcessingSuccess<S, E, Meta, Context, Message>>>>> batchProcessCommand(TxCtx ctx, List<C> commands) {
        // Collect all states from db
        return traverse(commands, c ->
                this.getSnapshot(ctx, c).thenCompose(mayBeState ->
                        //handle command with state to get events
                        handleCommand(ctx, mayBeState, c)
                                // Return command + state + (error or events)
                                .thenApply(r -> Tuple(c, mayBeState, r))
                )
        )
                .thenApply(Value::toList)
                .thenCompose(commandsAndResults -> {
                    // Extract errors from command handling
                    List<Either<Error, ProcessingSuccess<S, E, Meta, Context, Message>>> errors = commandsAndResults
                            .map(Tuple3::_3)
                            .filter(Either::isLeft)
                            .map(e -> Either.left(e.swap().get()));

                    // Extract success and generate envelopes for each result
                    CompletionStage<List<CommandStateAndEvent>> success = traverse(commandsAndResults.filter(t -> t._3.isRight()), t -> {
                        C command = t._1;
                        Option<S> mayBeState = t._2;
                        List<E> events = t._3.get().events.toList();
                        return buildEnvelopes(ctx, command, events).thenApply(eventEnvelopes -> {
                            Option<Long> mayBeLastSeqNum = eventEnvelopes.lastOption().map(evl -> evl.sequenceNum);
                            return new CommandStateAndEvent(command, mayBeState, eventEnvelopes, events, t._3.get().message, mayBeLastSeqNum);
                        });
                    });

                    return success.thenApply(s -> Tuple(s.toList(), errors));
                })
                .thenCompose(successAndErrors -> {

                    List<Either<Error, ProcessingSuccess<S, E, Meta, Context, Message>>> errors = successAndErrors._2;
                    List<CommandStateAndEvent> success = successAndErrors._1;

                    // Get all envelopes
                    List<EventEnvelope<E, Meta, Context>> envelopes = success.flatMap(CommandStateAndEvent::getEventEnvelopes);

                    CompletionStage<Seq<ProcessingSuccess<S, E, Meta, Context, Message>>> stored = eventStore
                            // Persist all envelopes
                            .persist(ctx, envelopes)
                            .thenCompose(__ ->
                                    // Persist states
                                    traverse(success, s -> {
                                        LOGGER.debug("Storing state {} to DB", s);
                                        return aggregateStore
                                                .buildAggregateAndStoreSnapshot(
                                                        ctx,
                                                        eventHandler,
                                                        s.getState(),
                                                        s.getCommand().entityId().get(),
                                                        s.getEvents(),
                                                        s.getSequenceNum()
                                                )
                                                .thenApply(mayBeNextState ->
                                                        new ProcessingSuccess<>(s.state, mayBeNextState, s.getEventEnvelopes(), s.getMessage())
                                                );
                                    })
                            )
                            .thenCompose(mayBeNextState ->
                                    // Apply events to projections
                                    traverse(projections, p -> {
                                        LOGGER.debug("Applying envelopes {} to projection", envelopes);
                                        return p.storeProjection(ctx, envelopes);
                                    })
                                            .thenApply(__ -> mayBeNextState)
                            );
                    return stored.thenApply(results ->
                            errors.appendAll(results.map(Either::right))
                    );
                })
                .thenApply(results -> new InTransactionResult<>(
                        results,
                        () -> {
                            List<EventEnvelope<E, Meta, Context>> envelopes = results.flatMap(Value::toList).flatMap(ProcessingSuccess::getEvents);
                            LOGGER.debug("Publishing events {} to kafka", envelopes);
                            return eventStore.publish(envelopes)
                                    .thenApply(__ -> Tuple.empty())
                                    .exceptionally(e -> Tuple.empty());
                        }
                ));
    }

    CompletionStage<List<EventEnvelope<E, Meta, Context>>> buildEnvelopes(TxCtx tx, C command, List<E> events) {
        String transactionId = transactionManager.transactionId();
        int nbMessages = events.length();
        return traverse(events.zipWithIndex(),
                t -> buildEnvelope(tx, command, t._1, t._2, nbMessages, transactionId)
        ).thenApply(Value::toList);
    }

    private CompletionStage<Either<Error, Events<E, Message>>> handleCommand(TxCtx txCtx, Option<S> state, C command) {
        return commandHandler.handleCommand(txCtx, state, command);
    }

    private CompletionStage<Option<S>> getSnapshot(TxCtx ctx, C command) {
        if (command.hasId()) {
            return aggregateStore.getAggregate(ctx, command.entityId().get());
        } else {
            return CompletionStages.successful(None());
        }
    }

    private CompletionStage<EventEnvelope<E, Meta, Context>> buildEnvelope(TxCtx tx, Command<Meta, Context> command, E event, Integer numMessage, Integer nbMessages, String transactionId) {
        LOGGER.debug("Writing event {} to envelope", event);


        UUID id = UUIDgen.generate();

        return eventStore.nextSequence(tx).thenApply(nextSequence -> {
            EventEnvelope.Builder<E, Meta, Context> builder = EventEnvelope.<E, Meta, Context>builder()
                    .withId(id)
                    .withEmissionDate(LocalDateTime.now())
                    .withEntityId(event.entityId())
                    .withSequenceNum(nextSequence)
                    .withEventType(event.type().name())
                    .withVersion(event.type().version())
                    .withTotalMessageInTransaction(nbMessages)
                    .withNumMessageInTransaction(numMessage + 1)
                    .withTransactionId(transactionId)
                    .withEvent(event);

            command.context().forEach(builder::withContext);
            command.userId().forEach(builder::withUserId);
            command.systemId().forEach(builder::withSystemId);
            command.metadata().forEach(builder::withMetadata);

            return builder.build();
        });
    }

    public CompletionStage<Option<S>> getAggregate(String id) {
        return transactionManager.withTransaction(t ->
                getAggregateStore().getAggregate(t, id)
        );
    }

    public EventStore<TxCtx, E, Meta, Context> eventStore() {
        return eventStore;
    }

    public AggregateStore<S, String, TxCtx> getAggregateStore() {
        return aggregateStore;
    }

    private class CommandStateAndEvent {
        public final C command;
        public final Option<S> state;
        public final List<EventEnvelope<E, Meta, Context>> eventEnvelopes;
        public final List<E> events;
        public final Message message;
        public final Option<Long> sequenceNum;

        public CommandStateAndEvent(C command, Option<S> state, List<EventEnvelope<E, Meta, Context>> eventEnvelopes, List<E> events, Message message, Option<Long> sequenceNum) {
            this.command = command;
            this.state = state;
            this.eventEnvelopes = eventEnvelopes;
            this.events = events;
            this.message = message;
            this.sequenceNum = sequenceNum;
        }

        public C getCommand() {
            return command;
        }

        public Option<S> getState() {
            return state;
        }

        public List<EventEnvelope<E, Meta, Context>> getEventEnvelopes() {
            return eventEnvelopes;
        }

        public List<E> getEvents() {
            return events;
        }

        public Message getMessage() {
            return message;
        }

        public Option<Long> getSequenceNum() {
            return sequenceNum;
        }
    }
}
