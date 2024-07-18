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
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static fr.maif.concurrent.CompletionStages.traverse;
import static io.vavr.API.*;
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
                .thenCompose(InTransactionResult::postTransaction);
    }

    record PreparedMessage<C extends Command<Meta, Context>, S extends State<S>, E extends Event, M, Meta, Context>(CommandAndInfos<C, S, E, M, Meta, Context> command, E event, Long seq, Integer num, Integer total, String transactionId) {}

    record CommandAndInfos<C extends Command<Meta, Context>, S extends State<S>, E extends Event, M, Meta, Context>(int tmpCommandId, C command, Option<S> mayBeState, Events<E, M> events) {
        @Override
        public boolean equals(Object obj) {
            if (obj instanceof CommandAndInfos<?, ?, ?, ?, ?, ?> commandAndInfos) {
                return tmpCommandId == commandAndInfos.tmpCommandId;
            }
            return false;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(tmpCommandId);
        }
    }

    @Override
    public CompletionStage<InTransactionResult<List<Either<Error, ProcessingSuccess<S, E, Meta, Context, Message>>>>> batchProcessCommand(TxCtx ctx, List<C> commands) {
        // Collect all states from db
        return aggregateStore.getAggregates(ctx, commands.filter(Command::hasId).map(c -> c.entityId().get()))
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
                                            .flatMap(Either::swap)
                                            .map(Either::left);
                                    AtomicInteger counter = new AtomicInteger(0);
                                    // Collect all successes : command + List<E>
                                    List<CommandAndInfos<C, S, E, Message, Meta, Context>> successes = commandsAndResults.flatMap(t -> t._3.map(events ->
                                            // Here we generate a tmp command id because we could have 2 commands for the same entity in the same batch
                                            new CommandAndInfos<>(counter.getAndIncrement(), t._1, t._2, events))
                                    );

                                    // Flatten by events : command + E (a join between command and events)
                                    List<PreparedMessage<C, S, E, Message, Meta, Context>> preparedMessages = successes.flatMap(commandAndInfos -> {
                                        String transactionId = transactionManager.transactionId();
                                        Integer totalMessages = commandAndInfos.events.events.size();
                                        return commandAndInfos.events.events.zipWithIndex().map(evt ->
                                                new PreparedMessage<>(commandAndInfos, evt._1, 0L, evt._2, totalMessages, transactionId)
                                        );
                                    });

                                    List<E> allEvents = successes.flatMap(t -> t.events.events);

                                    CompletionStage<List<CommandStateAndEvent>> success =
                                            // Generate sequences from DB
                                            eventStore.nextSequences(ctx, allEvents.size())
                                                    .thenApply(sequences ->
                                                            // apply a sequence to an event
                                                            preparedMessages.zip(sequences).map(t -> {
                                                                Long seq = t._2;
                                                                PreparedMessage<C, S, E, Message, Meta, Context> message = t._1;
                                                                // Build the envelope that will be stored in DB later
                                                                EventEnvelope<E, Meta, Context> eventEnvelope = buildEnvelope(ctx, message.command().command(), message.event(), seq, message.num(), message.total(), message.transactionId());
                                                                return Tuple(message.command, eventEnvelope);
                                                            })
                                                    )
                                                    .thenApply(allEnvelopes -> {
                                                        // group envelope by original command
                                                        Map<CommandAndInfos<C, S, E, Message, Meta, Context>, List<EventEnvelope<E, Meta, Context>>> indexedByCommandId = allEnvelopes
                                                                .groupBy(env -> env._1)
                                                                .mapValues(l -> l.map(t -> t._2));
                                                        // for each original command, we prepare the result that we be returned
                                                        return successes.map(commandAndInfos -> {
                                                            List<EventEnvelope<E, Meta, Context>> eventEnvelopes = indexedByCommandId.get(commandAndInfos).toList().flatMap(identity());
                                                            var mayBeLastSeqNum = eventEnvelopes.map(e -> e.sequenceNum).max();
                                                            return new CommandStateAndEvent(commandAndInfos.command, commandAndInfos.mayBeState, eventEnvelopes, commandAndInfos.events.events.toList(), commandAndInfos.events.message, mayBeLastSeqNum);
                                                        }).toList();

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
                                                                        new ProcessingSuccess<>(s.getState(), mayBeNextState, s.getEventEnvelopes(), s.getMessage())
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
                                    return new InTransactionResult<>(
                                            results,
                                            postTransactionProcess
                                    );
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

    private EventEnvelope<E, Meta, Context> buildEnvelope(TxCtx tx, Command<Meta, Context> command, E event, Long nextSequence, Integer numMessage, Integer nbMessages, String transactionId) {
        LOGGER.debug("Writing event {} to envelope", event);

        UUID id = UUIDgen.generate();

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
