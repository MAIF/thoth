package com.example.demo;

import akka.actor.ActorSystem;
import fr.maif.eventsourcing.*;
import fr.maif.eventsourcing.impl.DefaultAggregateStore;
import fr.maif.eventsourcing.impl.InMemoryEventStore;
import io.vavr.API;
import io.vavr.Lazy;
import io.vavr.Tuple;
import io.vavr.Tuple0;
import io.vavr.collection.List;
import io.vavr.concurrent.Future;
import io.vavr.control.Either;
import io.vavr.control.Option;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static io.vavr.API.*;
import static io.vavr.API.Case;

public class VikingApplication {
    public static class Viking implements State<Viking> {

        public final String id;
        public final String name;
        public Long sequenceNum;

        public Viking(String id, String name) {
            this.id = id;
            this.name = name;
        }

        @Override
        public Long sequenceNum() {
            return sequenceNum;
        }

        @Override
        public Viking withSequenceNum(Long sequenceNum) {
            this.sequenceNum = sequenceNum;
            return this;
        }
    }

    public interface VikingCommand extends Command<Tuple0, Tuple0> {

        Type<CreateViking> CreateVikingV1 = Type.create(CreateViking.class, 1L);
        Type<UpdateViking> UpdateVikingV1 = Type.create(UpdateViking.class, 1L);
        Type<DeleteViking> DeleteVikingV1 = Type.create(DeleteViking.class, 1L);

        class CreateViking implements VikingCommand {
            public String id;
            public String name;

            public CreateViking(String id, String name) {
                this.id = id;
                this.name = name;
            }

            @Override
            public Lazy<String> entityId() {
                return Lazy.of(() ->id);
            }
        }

        class UpdateViking implements VikingCommand {
            public String id;
            public String name;

            public UpdateViking(String id, String name) {
                this.id = id;
                this.name = name;
            }

            @Override
            public Lazy<String> entityId() {
                return Lazy.of(() ->id);
            }
        }

        class DeleteViking implements VikingCommand {
            public String id;

            public DeleteViking(String id) {
                this.id = id;
            }

            @Override
            public Lazy<String> entityId() {
                return Lazy.of(() ->id);
            }
        }
    }

    public interface VikingEvent extends Event {

        // Types are needed to handle event versionning :
        Type<VikingCreated> VikingCreatedV1 = Type.create(VikingCreated.class, 1L);
        Type<VikingUpdated> VikingUpdatedV1 = Type.create(VikingUpdated.class, 1L);
        Type<VikingDeleted> VikingDeletedV1 = Type.create(VikingDeleted.class, 1L);

        class VikingCreated implements VikingEvent {
            public String id;
            public String name;

            public VikingCreated(String id, String name) {
                this.id = id;
                this.name = name;
            }

            @Override
            public Type<VikingCreated> type() {
                return VikingCreatedV1;
            }

            @Override
            public String entityId() {
                return id;
            }
        }

        class VikingUpdated implements VikingEvent {
            public String id;
            public String name;

            public VikingUpdated(String id, String name) {
                this.id = id;
                this.name = name;
            }

            @Override
            public Type<VikingUpdated> type() {
                return VikingUpdatedV1;
            }

            @Override
            public String entityId() {
                return id;
            }
        }


        class VikingDeleted implements VikingEvent {
            public String id;

            public VikingDeleted(String id) {
                this.id = id;
            }

            @Override
            public Type<VikingDeleted> type() {
                return VikingDeletedV1;
            }

            @Override
            public String entityId() {
                return id;
            }
        }
    }

    public static class VikingCommandHandler implements CommandHandler<String, Viking, VikingCommand, VikingEvent, Tuple0, Tuple0> {
        @Override
        public Future<Either<String, Events<VikingEvent, Tuple0>>> handleCommand(Tuple0 tuple0, Option<Viking> option, VikingCommand vikingCommand) {
            return Future.successful(
                    // Here we pattern match on the type of the command
                    Match(vikingCommand).of(
                            Case(VikingCommand.CreateVikingV1.pattern(), e ->
                                    // Here we can add validation and reject the command if needed
                                    Right(Events.events(new VikingEvent.VikingCreated(e.id, e.name)))
                            ),
                            Case(VikingCommand.UpdateVikingV1.pattern(), e ->
                                    Right(Events.events(new VikingEvent.VikingUpdated(e.id, e.name)))
                            ),
                            Case(VikingCommand.DeleteVikingV1.pattern(), e ->
                                    Right(Events.events(new VikingEvent.VikingDeleted(e.id)))
                            )
                    )
            );
        }
    }

    public static class VikingEventHandler implements EventHandler<Viking, VikingEvent> {
        @Override
        public Option<Viking> applyEvent(Option<Viking> state, VikingEvent event) {
            return Match(event).of(
                    Case(VikingEvent.VikingCreatedV1.pattern(), e -> Option.of(new Viking(e.id, e.name))),
                    Case(VikingEvent.VikingUpdatedV1.pattern(), e -> Option.of(new Viking(e.id, e.name))),
                    Case(VikingEvent.VikingDeletedV1.pattern(), e -> Option.none())
            );
        }
    }

    public static class VikingProjection implements Projection<Tuple0, VikingEvent, Tuple0, Tuple0> {

        public ConcurrentHashMap<String, Viking> data = new ConcurrentHashMap<>();

        @Override
        public Future<Tuple0> storeProjection(Tuple0 unit, List<EventEnvelope<VikingEvent, Tuple0, Tuple0>> events) {
            return Future.of(() -> {
                events.forEach(event -> {
                    String entityId = event.entityId;
                    VikingEvent vikingEvent = event.event;
                    Option<Viking> viking = Match(vikingEvent).of(
                            Case(VikingEvent.VikingCreatedV1.pattern(), e -> Option.of(new Viking(e.id, e.name))),
                            Case(VikingEvent.VikingUpdatedV1.pattern(), e -> Option.of(new Viking(e.id, e.name))),
                            Case(VikingEvent.VikingDeletedV1.pattern(), e -> Option.none())
                    );
                    viking.forEach(v -> data.put(entityId, v));
                });
                return Tuple.empty();
            });
        }

        public Future<Option<Viking>> getById(String id) {
            return API.Future(API.Option(data.get(id)));
        }

        public Future<List<Viking>> findByName(String name) {
            return API.Future(
                    List.ofAll(data.values())
                            .filter(v -> v.name.equals(name))
            );
        }
    }

    public static class VikingAggregate implements AggregateStore<Viking, String, Tuple0> {
        @Override
        public Future<Option<Viking>> getAggregate(String s) {
            return null;
        }

        @Override
        public Future<Option<Viking>> getAggregate(Tuple0 tuple0, String s) {
            return null;
        }

        @Override
        public Future<Tuple0> storeSnapshot(Tuple0 tuple0, String s, Option<Viking> option) {
            return null;
        }
    }


    public static class Vikings {

        private final EventProcessor<String, Viking, VikingCommand, VikingEvent, Tuple0, Tuple0, Tuple0, Tuple0> eventProcessor;
        private final VikingProjection vikingReadModel = new VikingProjection();

        public Vikings(ActorSystem actorSystem) {
            InMemoryEventStore<Tuple0, VikingEvent, Tuple0, Tuple0> eventStore = InMemoryEventStore.create(actorSystem);
            VikingEventHandler eventHandler = new VikingEventHandler();
            TransactionManager<Tuple0> transactionManager = new TransactionManager<>() {
                @Override
                public <T> Future<T> withTransaction(Function<Tuple0, Future<T>> function) {
                    return function.apply(Tuple.empty());
                }
            };
            this.eventProcessor =  new EventProcessor<>(
                    eventStore,
                    transactionManager,
                    new DefaultAggregateStore<>(eventStore, eventHandler, actorSystem, transactionManager),
                    new VikingCommandHandler(),
                    eventHandler,
                    List.of(vikingReadModel)
            );
        }

        public Future<Either<String, ProcessingSuccess<Viking, VikingEvent, Tuple0, Tuple0, Tuple0>>> create(VikingCommand.CreateViking command) {
            return eventProcessor.processCommand(command);
        }

        public Future<Either<String, ProcessingSuccess<Viking, VikingEvent, Tuple0, Tuple0, Tuple0>>> update(VikingCommand.UpdateViking command) {
            return eventProcessor.processCommand(command);
        }

        public Future<Either<String, ProcessingSuccess<Viking, VikingEvent, Tuple0, Tuple0, Tuple0>>> delete(VikingCommand.DeleteViking command) {
            return eventProcessor.processCommand(command);
        }

        public Future<Option<Viking>> getById(String id) {
            return vikingReadModel.getById(id);
        }

        public Future<List<Viking>> findByName(String name) {
            return vikingReadModel.findByName(name);
        }

    }
}
