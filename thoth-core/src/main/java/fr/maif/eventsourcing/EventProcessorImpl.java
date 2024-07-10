package fr.maif.eventsourcing;

import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.impl.TimeBasedGenerator;
import fr.maif.concurrent.CompletionStages;
import fr.maif.eventsourcing.TransactionManager.InTransactionResult;
import io.vavr.Tuple;
import io.vavr.Tuple0;
import io.vavr.Tuple3;
import io.vavr.Value;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.collection.Seq;
import io.vavr.control.Either;
import io.vavr.control.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.vavr.API.List;
import static io.vavr.API.None;
import static io.vavr.API.Tuple;
import static fr.maif.concurrent.CompletionStages.traverse;
import static java.util.function.Function.identity;

public class EventProcessorImpl<Error, S extends State<S>, C extends Command<Meta, Context>, E extends Event, TxCtx, Message, Meta, Context> implements EventProcessor<Error, S, C, E, TxCtx, Message, Meta, Context> {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventProcessorImpl.class);
    private static final TimeBasedGenerator UUIDgen = Generators.timeBasedGenerator();

    private final EventStore<TxCtx, E, Meta, Context> eventStore;
    private final TransactionManager<TxCtx> transactionManager;

    private final AggregateStore<S, String, TxCtx> aggregateStore;
    private final CommandHandler<Error, S, C, E, Message, TxCtx> commandHandler;
    private final EventHandler<S, E> eventHandler;
    private final List<Projection<TxCtx, E, Meta, Context>> projections;

    public EventProcessorImpl(EventStore<TxCtx, E, Meta, Context> eventStore, TransactionManager<TxCtx> transactionManager, AggregateStore<S, String, TxCtx> aggregateStore, CommandHandler<Error, S, C, E, Message, TxCtx> commandHandler, EventHandler<S, E> eventHandler, List<Projection<TxCtx, E, Meta, Context>> projections) {
        this.eventStore = eventStore;
        this.transactionManager = transactionManager;
        this.aggregateStore = aggregateStore;
        this.commandHandler = commandHandler;
        this.eventHandler = eventHandler;
        this.projections = projections;
    }

    @Override
    public CompletionStage<Either<Error, ProcessingSuccess<S, E, Meta, Context, Message>>> processCommand(C command) {
        LOGGER.debug("Processing command {}", command);
        return batchProcessCommand(List(command)).thenApply(List::head);
    }

    @Override
    public CompletionStage<List<Either<Error, ProcessingSuccess<S, E, Meta, Context, Message>>>> batchProcessCommand(List<C> commands) {
        LOGGER.debug("Processing commands {}", commands);
        return transactionManager
                .withTransaction(ctx -> batchProcessCommand(ctx, commands))
                .thenCompose(listInTransactionResult ->
                        listInTransactionResult.postTransaction()
                );
    }

    @Override
    public CompletionStage<InTransactionResult<List<Either<Error, ProcessingSuccess<S, E, Meta, Context, Message>>>>> batchProcessCommand(TxCtx ctx, List<C> commands) {
        // Collect all states from db
        return aggregateStore.getAggregates(ctx, commands.filter(c -> c.hasId()).map(c -> c.entityId().get()))
                .thenCompose(states ->
                        traverseCommands(commands, (c, events) -> {
                            //handle command with state to get events
                            Option<S> mayBeState = this.getCurrentState(ctx, states, c, events);
                            return handleCommand(ctx, mayBeState, c)
                                    // Return command + state + (error or events)
                                    .thenApply(r -> Tuple(c, mayBeState, r));
                        })
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
                                                        List<Long> sequences = envelopes.filter(env -> env.entityId.equals(s.command.entityId().get())).map(env -> env.sequenceNum);
                                                        return aggregateStore
                                                                .buildAggregateAndStoreSnapshot(
                                                                        ctx,
                                                                        eventHandler,
                                                                        s.getState(),
                                                                        s.getCommand().entityId().get(),
                                                                        s.getEvents(),
                                                                        sequences.max()
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
                                .thenApply(results -> {
                                    Supplier<CompletionStage<Tuple0>> postTransactionProcess = () -> {
                                        List<EventEnvelope<E, Meta, Context>> envelopes = results.flatMap(Value::toList).flatMap(ProcessingSuccess::getEvents);
                                        LOGGER.debug("Publishing events {} to kafka", envelopes);
                                        return eventStore.publish(envelopes)
                                                .thenApply(__ -> Tuple.empty())
                                                .exceptionally(e -> Tuple.empty());
                                    };
                                    var inTransactionResult = new InTransactionResult<>(
                                            results,
                                            postTransactionProcess
                                    );
                                    return inTransactionResult;
                                })
                );
    }

    public CompletionStage<List<Tuple3<C, Option<S>, Either<Error, Events<E, Message>>>>> traverseCommands(List<C> elements, BiFunction<C, List<E>, CompletionStage<Tuple3<C, Option<S>, Either<Error, Events<E, Message>>>>> handler) {
        return elements.foldLeft(
                CompletionStages.completedStage(Tuple(List.<Tuple3<C, Option<S>, Either<Error, Events<E, Message>>>>empty(), List.<Events<E, Message>>empty())),
                (fResult, elt) ->
                        fResult.thenCompose(listResult -> handler.apply(elt, listResult._2.flatMap(e -> e.events))
                                .thenApply(r ->
                                        Tuple(
                                                listResult._1.append(r),
                                                listResult._2.append(r._3.getOrElse(Events.empty())))
                                ))
        ).thenApply(t -> t._1);
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

    private Option<S> getCurrentState(TxCtx ctx, Map<String, Option<S>> states, C command, List<E> previousEvent) {
        if (command.hasId()) {
            String entityId = command.entityId().get();
            return eventHandler.deriveState(states.get(entityId).flatMap(identity()), previousEvent.filter(e -> e.entityId().equals(entityId)));
        } else {
            return None();
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

    @Override
    public CompletionStage<Option<S>> getAggregate(String id) {
        return transactionManager.withTransaction(t ->
                getAggregateStore().getAggregate(t, id)
        );
    }

    @Override
    public EventStore<TxCtx, E, Meta, Context> eventStore() {
        return eventStore;
    }

    @Override
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
