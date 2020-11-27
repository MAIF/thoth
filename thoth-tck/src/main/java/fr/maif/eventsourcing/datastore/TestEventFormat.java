package fr.maif.eventsourcing.datastore;

import com.fasterxml.jackson.databind.JsonNode;
import fr.maif.eventsourcing.format.JacksonEventFormat;
import fr.maif.json.Json;
import fr.maif.json.JsonWrite;
import io.vavr.API;
import io.vavr.Tuple;
import io.vavr.control.Either;

import static io.vavr.API.Case;

public class TestEventFormat implements JacksonEventFormat<String, TestEvent> {
    @Override
    public Either<String, TestEvent> read(String type, Long version, JsonNode json) {
        return API.Match(Tuple.of(type, version)).option(
                Case(TestEvent.SimpleEventV1.pattern2(), () -> Json.fromJson(json, TestEvent.SimpleEvent.class)),
                Case(TestEvent.DeleteEventV1.pattern2(), () -> Json.fromJson(json, TestEvent.DeleteEvent.class))
        )
        .toEither(() -> "Unknown event type " + type + "(v" + version + ")")
        .flatMap(jsResult -> jsResult.toEither().mapLeft(errs -> errs.mkString(",")));
    }

    @Override
    public JsonNode write(TestEvent json) {
        return Json.toJson(json, JsonWrite.auto());
    }
}
