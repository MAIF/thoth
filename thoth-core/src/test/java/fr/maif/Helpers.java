package fr.maif;

import com.fasterxml.jackson.databind.JsonNode;
import fr.maif.concurrent.CompletionStages;
import fr.maif.eventsourcing.*;
import fr.maif.eventsourcing.format.JacksonEventFormat;
import fr.maif.json.EventEnvelopeJsonFormat;
import fr.maif.json.JsonRead;
import fr.maif.json.JsonSchema;
import fr.maif.json.JsonWrite;
import io.vavr.API;
import io.vavr.Lazy;
import io.vavr.Tuple;
import io.vavr.Tuple0;
import io.vavr.Tuple2;
import io.vavr.collection.List;
import io.vavr.control.Either;
import io.vavr.control.Option;

import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

import static fr.maif.Helpers.VikingEvent.VikingCreatedV1;
import static fr.maif.Helpers.VikingEvent.VikingDeletedV1;
import static fr.maif.Helpers.VikingEvent.VikingUpdatedV1;
import static fr.maif.json.JsonRead._fromClass;
import static fr.maif.json.JsonRead.ofRead;
import static io.vavr.API.*;
import static io.vavr.Patterns.$None;
import static io.vavr.Patterns.$Some;

public class Helpers {


    public interface VikingEvent extends Event {

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

            public VikingCreated() {
            }

            @Override
            public Type<VikingCreated> type() {
                return VikingCreatedV1;
            }

            @Override
            public String entityId() {
                return id;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                VikingCreated that = (VikingCreated) o;
                return Objects.equals(id, that.id) &&
                        Objects.equals(name, that.name);
            }

            @Override
            public int hashCode() {
                return Objects.hash(id, name);
            }

            @Override
            public String toString() {
                return new StringJoiner(", ", VikingCreated.class.getSimpleName() + "[", "]")
                        .add("id='" + id + "'")
                        .add("name='" + name + "'")
                        .toString();
            }
        }

        class VikingUpdated implements VikingEvent {
            public String id;
            public String name;

            public VikingUpdated(String id, String name) {
                this.id = id;
                this.name = name;
            }

            public VikingUpdated() {
            }

            @Override
            public Type<VikingUpdated> type() {
                return VikingUpdatedV1;
            }

            @Override
            public String entityId() {
                return id;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                VikingUpdated that = (VikingUpdated) o;
                return Objects.equals(id, that.id) &&
                        Objects.equals(name, that.name);
            }

            @Override
            public int hashCode() {
                return Objects.hash(id, name);
            }

            @Override
            public String toString() {
                return new StringJoiner(", ", VikingUpdated.class.getSimpleName() + "[", "]")
                        .add("id='" + id + "'")
                        .add("name='" + name + "'")
                        .toString();
            }
        }


        class VikingDeleted implements VikingEvent {
            public String id;

            public VikingDeleted(String id) {
                this.id = id;
            }

            public VikingDeleted() {
            }

            @Override
            public Type<VikingDeleted> type() {
                return VikingDeletedV1;
            }

            @Override
            public String entityId() {
                return id;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                VikingDeleted that = (VikingDeleted) o;
                return Objects.equals(id, that.id);
            }

            @Override
            public int hashCode() {
                return Objects.hash(id);
            }

            @Override
            public String toString() {
                return new StringJoiner(", ", VikingDeleted.class.getSimpleName() + "[", "]")
                        .add("id='" + id + "'")
                        .toString();
            }
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

    public static class VikingEventHandler implements EventHandler<Viking, VikingEvent> {
        @Override
        public Option<Viking> applyEvent(Option<Viking> state, VikingEvent event) {
            return Match(event).of(
                    Case(VikingCreatedV1.pattern(), e -> Option.of(new Viking(e.id, e.name))),
                    Case(VikingUpdatedV1.pattern(), e -> Option.of(new Viking(e.id, e.name))),
                    Case(VikingDeletedV1.pattern(), e -> Option.none())
            );
        }
    }

    public static class VikingCommandHandler implements CommandHandler<String, Viking, VikingCommand, VikingEvent, String, Tuple0> {

        @Override
        public CompletionStage<Either<String, Events<VikingEvent, String>>> handleCommand(Tuple0 unit, Option<Viking> state, VikingCommand vikingCommand) {
            return CompletionStages.completedStage(
                    Match(vikingCommand).of(
                            Case(VikingCommand.CreateVikingV1.pattern(), e -> events("C", new VikingEvent.VikingCreated(e.id, e.name))),
                            Case(VikingCommand.UpdateVikingV1.pattern(), e -> events("U", new VikingEvent.VikingUpdated(e.id, e.name))),
                            Case(VikingCommand.DeleteVikingV1.pattern(), e -> events("D", new VikingEvent.VikingDeleted(e.id)))
                    )
            );
        }
    }

    public static class VikingEventFormat implements JacksonEventFormat<String, VikingEvent> {
        @Override
        public Either<String, VikingEvent> read(String type, Long version, JsonNode json) {
            return Match(API.Tuple(type, version))
                    .option(
                            Case(VikingCreatedV1.pattern2(), (t, v) -> Json.fromJson(json, VikingEvent.VikingCreated.class)),
                            Case(VikingUpdatedV1.pattern2(), (t, v) -> Json.fromJson(json, VikingEvent.VikingUpdated.class)),
                            Case(VikingDeletedV1.pattern2(), (t, v) -> Json.fromJson(json, VikingEvent.VikingDeleted.class))
                    )
                    .toEither("Not implemented");
        }

        @Override
        public JsonNode write(VikingEvent json) {
            return Json.toJson(json);
        }
    }

    public static class VikingEventJsonFormat implements EventEnvelopeJsonFormat<VikingEvent, Tuple0, Tuple0> {

        @Override
        public List<Tuple2<Type<? extends VikingEvent>, JsonRead<? extends VikingEvent>>> cases() {
            return List(
                Tuple(VikingCreatedV1, ofRead(_fromClass(VikingEvent.VikingCreated.class), JsonSchema.objectSchema()).description("VikingCreated")),
                Tuple(VikingUpdatedV1, ofRead(_fromClass(VikingEvent.VikingUpdated.class), JsonSchema.objectSchema()).description("VikingUpdated")),
                Tuple(VikingDeletedV1, ofRead(_fromClass(VikingEvent.VikingDeleted.class), JsonSchema.objectSchema()).description("VikingDeleted"))
            );
        }

        @Override
        public JsonWrite<VikingEvent> eventWrite() {
            return JsonWrite.auto();
        }

    }

    public static class VikingSnapshot implements SnapshotStore<Viking, String, Tuple0> {

        public ConcurrentHashMap<String, Viking> data = new ConcurrentHashMap<>();


        @Override
        public CompletionStage<Option<Viking>> getSnapshot(String entityId) {
            return CompletionStages.completedStage(Option.of(data.get(entityId)));
        }

        @Override
        public CompletionStage<Option<Viking>> getSnapshot(Tuple0 transactionContext, String entityId) {
            return CompletionStages.completedStage(Option.of(data.get(entityId)));
        }

        @Override
        public CompletionStage<Tuple0> persist(Tuple0 transactionContext, String id, Option<Viking> state) {
            Match(state).of(
                    Case($Some($()), s -> data.put(s.id, s)),
                    Case($None(), () -> data.remove(id))
            );
            return CompletionStages.completedStage(Tuple.empty());
        }
    }

    public static class VikingProjection implements Projection<Tuple0, VikingEvent, Tuple0, Tuple0> {
        public ConcurrentHashMap<String, Integer> data = new ConcurrentHashMap<>();
        @Override
        public CompletionStage<Tuple0> storeProjection(Tuple0 unit, List<EventEnvelope<VikingEvent, Tuple0, Tuple0>> events) {
            events.forEach(event -> {
                int i = data.getOrDefault(event.entityId, 0) + events.size();
                data.put(event.entityId, i);
            });
            return CompletionStages.empty();
        }
    }

    public static class Viking implements State<Viking> {

        String id;
        String name;
        Long sequenceNum;

        public Viking(String id, String name) {
            this.id = id;
            this.name = name;
        }

        public Viking(String id, String name, Long sequenceNum) {
            this.id = id;
            this.name = name;
            this.sequenceNum = sequenceNum;
        }

        @Override
        public Long sequenceNum() {
            return sequenceNum;
        }

        @Override
        public String entityId() {
            return id;
        }

        @Override
        public Viking withSequenceNum(Long sequenceNum) {
            this.sequenceNum = sequenceNum;
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Viking viking = (Viking) o;
            return Objects.equals(id, viking.id) &&
                    Objects.equals(name, viking.name) &&
                    Objects.equals(sequenceNum, viking.sequenceNum);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, name, sequenceNum);
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", Viking.class.getSimpleName() + "[", "]")
                    .add("id='" + id + "'")
                    .add("name='" + name + "'")
                    .add("sequenceNum=" + sequenceNum)
                    .toString();
        }
    }
}
