package fr.maif.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import fr.maif.eventsourcing.Event;
import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.eventsourcing.Type;
import fr.maif.eventsourcing.format.JacksonEventFormat;
import io.vavr.API;
import io.vavr.Tuple2;
import io.vavr.collection.List;
import io.vavr.collection.Seq;
import io.vavr.control.Either;
import io.vavr.control.Option;

import java.util.UUID;
import java.util.function.Function;

import static fr.maif.json.Json.$$;
import static fr.maif.json.JsonRead.*;
import static fr.maif.json.JsonWrite.$localdatetime;

public interface EventEnvelopeJsonFormat<E extends Event, Meta, Context> extends JsonFormat<EventEnvelope<E, Meta, Context>> {

    List<Tuple2<Type<? extends E>, JsonRead<? extends E>>> cases();

    JsonWrite<E> eventWrite();


    static <T> JsonFormat<T> emptyFormat() {
        return JsonFormat.of(
                json -> JsResult.success(null),
                __ -> NullNode.getInstance()
        );
    }

    default List<ReadCase<Tuple2<String, Long>, E>> casesOf() {
        return cases().map(t -> caseOf(t._1.pattern2(), ((JsonRead<E>) t._2).mapSchema(s -> s.title(t._1.name()))));
    }

    default List<String> eventTypes() {
        return cases().map(t -> t._1).map(Type::name);
    }

    default List<Long> eventVersions() {
        return cases().map(t -> t._1).map(Type::version).distinct();
    }


    @Override
    default JsResult<EventEnvelope<E, Meta, Context>> read(JsonNode jsonNode) {
        return jsonRead().read(jsonNode);
    }

    @Override
    default JsonNode write(EventEnvelope<E, Meta, Context> value) {
        return jsonWrite().write(value);
    }

    @Override
    default JsonSchema jsonSchema() {
        return this.jsonRead().jsonSchema();
    }

    @Override
    default JsonRead<EventEnvelope<E, Meta, Context>> jsonRead() {
        return eventRead().map(EventEnvelope.<E, Meta, Context>builder()::withEvent)
                .and(__("id", _string().map(UUID::fromString)), EventEnvelope.Builder::withId)
                .and(__("sequenceNum", _long().description("The incremental sequence number of the event")), EventEnvelope.Builder::withSequenceNum)
                .and(__("eventType", _string().mapSchema(s -> JsonSchema.enumSchema(eventTypes()).description("The schema type of the event"))), EventEnvelope.Builder::withEventType)
                .and(__("version", _long().description("The schema version of the event")), EventEnvelope.Builder::withVersion)
                .and(_nullable("emissionDate", _isoLocalDateTime().description("Date of the creation of this event")), EventEnvelope.Builder::withEmissionDate)
                .and(_nullable("transactionId", _string().description("A unique id that represent the transaction")), EventEnvelope.Builder::withTransactionId)
                .and(_nullable("metadata", metaFormat().description("Metadata associated with event")), EventEnvelope.Builder::withMetadata)
                .and(_nullable("context", contextFormat().description("Context associated with event")), EventEnvelope.Builder::withContext)
                .and(_nullable("published", _boolean()), EventEnvelope.Builder::withPublished)
                .and(_nullable("totalMessageInTransaction", _int().description("Total count of events generated in the transaction")), EventEnvelope.Builder::withTotalMessageInTransaction)
                .and(_nullable("numMessageInTransaction", _int().description("Current count for the event in the transaction")), EventEnvelope.Builder::withNumMessageInTransaction)
                .and(_nullable("entityId", _string().description("The entity id")), EventEnvelope.Builder::withEntityId)
                .and(_nullable("userId", _string().description("The id of the user that generate this event")), EventEnvelope.Builder::withUserId)
                .and(_nullable("systemId", _string().description("The id of the system that generate this event")), EventEnvelope.Builder::withSystemId)
                .map(EventEnvelope.Builder::build)
                .title("Event envelope")
                .description("Event envelope");
    }

    @Override
    default JsonWrite<EventEnvelope<E, Meta, Context>> jsonWrite() {
        return envelope -> Json.obj(
                $$("id", Option.of(envelope.id).map(UUID::toString)),
                $$("sequenceNum", envelope.sequenceNum),
                $$("eventType", envelope.eventType),
                $$("version", envelope.version),
                $$("emissionDate", envelope.emissionDate, $localdatetime()),
                $$("transactionId", envelope.transactionId),
                $$("metadata", envelope.metadata, metaFormat()),
                $$("event", envelope.event, eventWrite()),
                $$("context", envelope.context, contextFormat()),
                $$("published", envelope.published),
                $$("totalMessageInTransaction", envelope.totalMessageInTransaction),
                $$("numMessageInTransaction", envelope.numMessageInTransaction),
                $$("entityId", envelope.entityId),
                $$("userId", envelope.userId),
                $$("systemId", envelope.systemId)
        );
    }

    default JsonFormat<Meta> metaFormat() {
        return emptyFormat();
    }

    default JsonFormat<Context> contextFormat() {
        return emptyFormat();
    }


    default JsonRead<E> eventRead() {
        return JsonRead.oneOf(
                        __("eventType", _string()),
                        __("version", _long()),
                        "event",
                        casesOf().map(c -> c.map(r -> (JsonRead<E>) r))
                ).description("one of the event based on eventType et version");
    }

    default JacksonEventFormat<Seq<JsResult.Error>, E> jacksonEventFormat() {
        return jacksonEventFormat(Function.identity());
    }

    default <Err> JacksonEventFormat<Err, E> jacksonEventFormat(Function<Seq<JsResult.Error>, Err> convertErrors) {
        return new JacksonEventFormat<Err, E>() {

            @Override
            public Either<Err, E> read(String type, Long version, JsonNode json) {
                Tuple2<String, Long> t = API.Tuple(type, version);
                Option<JsonRead<E>> jsonReads = casesOf().foldLeft(Option.<JsonRead<E>>none(), (curr, elt) -> {
                    if (curr.isDefined()) {
                        return curr;
                    } else {
                        return elt.jsonRead(t).map(r -> (JsonRead<E>)r);
                    }
                });
                return jsonReads.get().read(json).toEither().mapLeft(convertErrors);
            }

            @Override
            public JsonNode write(E json) {
                return eventWrite().write(json);
            }
        };
    }

}
