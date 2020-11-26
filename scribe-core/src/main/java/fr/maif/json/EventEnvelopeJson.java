package fr.maif.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import fr.maif.eventsourcing.Event;
import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.eventsourcing.format.JacksonEventFormat;
import fr.maif.eventsourcing.format.JacksonSimpleFormat;
import io.vavr.control.Either;
import io.vavr.control.Option;
import io.vavr.control.Try;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class EventEnvelopeJson {

    private final static ObjectMapper mapper = MapperSingleton.getInstance();

    public static <E extends Event, Meta, Context> String serializeToString(EventEnvelope<E, Meta, Context> event,
                                                                            JacksonEventFormat<?, E> format,
                                                                            JacksonSimpleFormat<Meta> metaFormat,
                                                                            JacksonSimpleFormat<Context> contextFormat) {
        return Try.of(() -> mapper.writer().writeValueAsString(serialize(event, format, metaFormat, contextFormat))).get();
    }

    public static <E extends Event, Meta, Context> JsonNode serialize(EventEnvelope<E, Meta, Context> event,
                                                                      JacksonEventFormat<?, E> format,
                                                                      JacksonSimpleFormat<Meta> metaFormat,
                                                                      JacksonSimpleFormat<Context> contextFormat) {
        JsonNode jsonEvent = format.write(event.event);
        ObjectNode jsonNodes = mapper.valueToTree(event);
        Option<JsonNode> maybeData = metaFormat.write(Option.of(event.metadata));
        Option<JsonNode> maybeContext = contextFormat.write(Option.of(event.context));
        jsonNodes.set("event", jsonEvent);
        maybeData.forEach(n -> jsonNodes.set("metadata", n));
        maybeContext.forEach(n -> jsonNodes.set("context", n));
        return jsonNodes;
    }

    public static <E extends Event, Meta, Context> EventEnvelope<E, Meta, Context> deserialize(
            String event,
            JacksonEventFormat<?, E> format,
            JacksonSimpleFormat<Meta> metaFormat,
            JacksonSimpleFormat<Context> contextFormat,
            BiConsumer<String, Object> onError,
            Consumer<EventEnvelope<E, Meta, Context>> onSuccess) {

        try {
            ObjectNode jsonNode = (ObjectNode) mapper.reader().readTree(event);
            return deserialize(jsonNode, format, metaFormat, contextFormat, onError, onSuccess);
        } catch (Exception e) {
            try {
                onError.accept(mapper.writer().writeValueAsString(event), e);
            } catch (JsonProcessingException e1) {
            }
            return null;
        }
    }

    public static <E extends Event, Meta, Context> EventEnvelope<E, Meta, Context> deserialize(
            ObjectNode event,
            JacksonEventFormat<?, E> format,
            JacksonSimpleFormat<Meta> metaFormat,
            JacksonSimpleFormat<Context> contextFormat,
            BiConsumer<String, Object> onError,
            Consumer<EventEnvelope<E, Meta, Context>> onSuccess) {

        try {
            JsonNode eventNode = event.get("event");
            JsonNode contextNode = event.get("context");
            JsonNode metaNode = event.get("metadata");
            event.set("event", NullNode.getInstance());
            event.set("context", NullNode.getInstance());
            event.set("metadata", NullNode.getInstance());

            EventEnvelope.Builder<E, Meta, Context> eventEnvelope = mapper.convertValue(event, new TypeReference<EventEnvelope.Builder<E, Meta, Context>>() {
            });
            Either<?, E> read = format.read(eventEnvelope.eventType, eventEnvelope.version, eventNode);
            read.mapLeft(err -> {
                try {
                    onError.accept(mapper.writer().writeValueAsString(event), err);
                } catch (JsonProcessingException e) {
                }
                return err;
            });
            eventEnvelope.withEvent(read.getOrNull());
            metaFormat.read(Option.of(metaNode)).forEach(eventEnvelope::withMetadata);
            contextFormat.read(Option.of(contextNode)).forEach(eventEnvelope::withContext);
            EventEnvelope<E, Meta, Context> build = eventEnvelope.build();
            onSuccess.accept(build);
            return build;
        } catch (Exception e) {
            try {
                onError.accept(mapper.writer().writeValueAsString(event), e);
            } catch (JsonProcessingException e1) {
            }
            return null;
        }
    }

}
