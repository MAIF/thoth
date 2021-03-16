package fr.maif.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import fr.maif.Helpers;
import io.vavr.Tuple0;
import fr.maif.eventsourcing.EventEnvelope;
import io.vavr.control.Either;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.UUID;

import static fr.maif.Helpers.VikingEvent.VikingCreatedV1;
import static fr.maif.json.Json.$$;
import static fr.maif.json.JsonWrite.$localdatetime;
import static org.assertj.core.api.Assertions.assertThat;

public class EventEnvelopeJsonFormatTest {
    UUID uuid = UUID.randomUUID();
    LocalDateTime now = LocalDateTime.now();
    Helpers.VikingEventJsonFormat eventReader = new Helpers.VikingEventJsonFormat();

    Helpers.VikingEvent.VikingCreated event = new Helpers.VikingEvent.VikingCreated("1", "ragnar");
    EventEnvelope<Helpers.VikingEvent, Tuple0, Tuple0> eventEnvelope = EventEnvelope.<Helpers.VikingEvent, Tuple0, Tuple0>builder()
            .withId(uuid)
            .withSequenceNum(1L)
            .withEventType(VikingCreatedV1.name())
            .withEmissionDate(now)
            .withTransactionId("1")
            .withEvent(event)
            .withVersion(VikingCreatedV1.version())
            .withPublished(true)
            .withTotalMessageInTransaction(1)
            .withNumMessageInTransaction(1)
            .withEntityId("1")
            .withUserId("user1")
            .withSystemId("system1")
            .build();

    ObjectNode eventJson = Json.obj(
            $$("id", "1"),
            $$("name", "ragnar")
    );

    JsonNode eventEnvelopeJson = Json.obj(
            $$("id", uuid.toString()),
            $$("entityId", "1"),
            $$("sequenceNum", 1L),
            $$("emissionDate", now, $localdatetime()),
            $$("eventType", VikingCreatedV1.name()),
            $$("version", VikingCreatedV1.version()),
            $$("totalMessageInTransaction", 1),
            $$("numMessageInTransaction", 1),
            $$("transactionId", "1"),
            $$("published", true),
            $$("userId", "user1"),
            $$("systemId", "system1"),
            $$("event", eventJson)
    );


    @Test
    public void readEnvelope() {

        JsResult<EventEnvelope<Helpers.VikingEvent, Tuple0, Tuple0>> read = eventReader.read(eventEnvelopeJson);
        assertThat(read.get()).isEqualTo(eventEnvelope);
    }

    @Test
    public void writeEnvelope() {

        JsonNode json = eventReader.write(eventEnvelope);

        assertThat(json).isEqualTo(eventEnvelopeJson);
    }

    @Test
    public void jsonSchema() {
        JsonSchema jsonSchema = eventReader.jsonSchema();

        ObjectNode expectedSchema = Json.obj(
                $$("type", "object"),
                $$("required", Json.arr("event", "eventType", "id", "sequenceNum", "version")),
                $$("properties", Json.obj(
                        $$("systemId", Json.obj(
                                $$("type", "string"),
                                $$("description", "The id of the system that generate this event"),
                                $$("exemples", Json.newArray())
                        )),
                        $$("userId", Json.obj(
                                $$("type", "string"),
                                $$("description", "The id of the user that generate this event"),
                                $$("exemples", Json.newArray())
                        )),
                        $$("entityId", Json.obj(
                                $$("type", "string"),
                                $$("description", "The entity id"),
                                $$("exemples", Json.newArray())
                        )),
                        $$("numMessageInTransaction", Json.obj(
                                $$("type", "integer"),
                                $$("description", "Current count for the event in the transaction"),
                                $$("exemples", Json.newArray())
                        )),
                        $$("totalMessageInTransaction", Json.obj(
                                $$("type", "integer"),
                                $$("description", "Total count of events generated in the transaction"),
                                $$("exemples", Json.newArray())
                        )),
                        $$("published", Json.obj(
                                $$("type", "boolean"))
                        ),
                        $$("context", Json.obj(
                                $$("description", "Context associated with event"),
                                $$("exemples", Json.newArray())
                        )),
                        $$("metadata", Json.obj(
                                $$("description", "Metadata associated with event"),
                                $$("exemples", Json.newArray())
                        )),
                        $$("transactionId", Json.obj(
                                $$("type", "string"),
                                $$("description", "A unique id that represent the transaction"),
                                $$("exemples", Json.newArray())
                        )),
                        $$("emissionDate", Json.obj(
                                $$("type", "string"),
                                $$("format", "date-time"),
                                $$("description", "Date of the creation of this event"),
                                $$("exemples", Json.newArray())
                        )),
                        $$("version", Json.obj(
                                $$("type", "number"),
                                $$("description", "The schema version of the event"),
                                $$("exemples", Json.newArray())
                        )),
                        $$("eventType", Json.obj(
                                $$("type", "string"),
                                $$("enum", Json.arr("VikingCreated","VikingUpdated","VikingDeleted")),
                                $$("description", "The schema type of the event"),
                                $$("exemples", Json.newArray())
                        )),
                        $$("sequenceNum", Json.obj(
                                $$("type", "number"),
                                $$("description", "The incremental sequence number of the event"),
                                $$("exemples", Json.newArray())
                        )),
                        $$("id", Json.obj(
                                $$("type", "string")
                        )),
                        $$("event", Json.obj(
                                $$("oneOf", Json.arr(
                                        Json.obj(
                                                $$("type", "object"),
                                                $$("title", "VikingCreated"),
                                                $$("description", "VikingCreated"),
                                                $$("exemples", Json.newArray())
                                        ),
                                        Json.obj(
                                                $$("type", "object"),
                                                $$("title", "VikingUpdated"),
                                                $$("description", "VikingUpdated"),
                                                $$("exemples", Json.newArray())
                                        ),
                                        Json.obj(
                                                $$("type", "object"),
                                                $$("title", "VikingDeleted"),
                                                $$("description", "VikingDeleted"),
                                                $$("exemples", Json.newArray())
                                        )
                                )),
                                $$("description", "one of the event based on eventType et version"),
                                $$("exemples", Json.newArray())
                        ))
                )),
                $$("title", "Event envelope"),
                $$("description", "Event envelope"),
                $$("exemples", Json.newArray())
        );

        assertThat(jsonSchema.toJson()).isEqualTo(expectedSchema);
    }

    @Test
    public void legacyMethod() {
        Either<String, Helpers.VikingEvent> read = eventReader.jacksonEventFormat(seq -> seq.map(e -> e.message).mkString(", ")).read(VikingCreatedV1.name(), VikingCreatedV1.version(), eventJson);
        assertThat(read).isEqualTo(Either.right(event));
    }
}