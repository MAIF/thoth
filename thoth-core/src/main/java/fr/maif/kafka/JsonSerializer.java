package fr.maif.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import fr.maif.eventsourcing.Event;
import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.eventsourcing.format.EventFormat;
import fr.maif.eventsourcing.format.JacksonEventFormat;
import fr.maif.eventsourcing.format.JacksonSimpleFormat;
import fr.maif.eventsourcing.format.SimpleFormat;
import fr.maif.json.EventEnvelopeJson;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;

public class JsonSerializer<E extends Event, Meta, Context> implements Serializer<EventEnvelope<E, Meta, Context>> {

    private final StringSerializer stringSerializer;
    private final JacksonEventFormat<?, E> eventFormat;
    private final JacksonSimpleFormat<Meta> metaFormat;
    private final JacksonSimpleFormat<Context> contextFormat;

    public JsonSerializer(JacksonEventFormat<?, E> eventFormat, JacksonSimpleFormat<Meta> metaFormat, JacksonSimpleFormat<Context> contextFormat) {
        this.eventFormat = eventFormat;
        this.metaFormat = metaFormat;
        this.contextFormat = contextFormat;
        this.stringSerializer = new StringSerializer();
    }

    public static <E extends Event, Meta, Context> JsonSerializer<E, Meta, Context> of(JacksonEventFormat<?, E> eventFormat) {
        return new JsonSerializer<>(eventFormat, JacksonSimpleFormat.empty(), JacksonSimpleFormat.empty());
    }

    public static <E extends Event, Meta, Context> JsonSerializer<E, Meta, Context> of(fr.maif.eventsourcing.vanilla.format.JacksonEventFormat<?, E> eventFormat) {
        return new JsonSerializer<>(eventFormat.toFormat(), JacksonSimpleFormat.empty(), JacksonSimpleFormat.empty());
    }

    public static <E extends Event, Meta, Context> JsonSerializer<E, Meta, Context> of(JacksonEventFormat<?, E> eventFormat, JacksonSimpleFormat<Meta> metaFormat, JacksonSimpleFormat<Context> contextFormat) {
        return new JsonSerializer<>(eventFormat, metaFormat, contextFormat);
    }

    public static <E extends Event, Meta, Context> JsonSerializer<E, Meta, Context> of(fr.maif.eventsourcing.vanilla.format.JacksonEventFormat<?, E> eventFormat, fr.maif.eventsourcing.vanilla.format.JacksonSimpleFormat<Meta> metaFormat, fr.maif.eventsourcing.vanilla.format.JacksonSimpleFormat<Context> contextFormat) {
        JacksonSimpleFormat<Meta> simpleFormat = metaFormat.toFormat();
        JacksonSimpleFormat<Context> simpleFormat1 = contextFormat.toFormat();
        JacksonEventFormat<?, E> format = eventFormat.toFormat();
        return new JsonSerializer<>(format, simpleFormat, simpleFormat1);
    }

    public static <E extends Event> JsonSerializer<E, JsonNode, JsonNode> ofJsonCtxMeta(JacksonEventFormat<?, E> eventFormat) {
        return new JsonSerializer<>(eventFormat, JacksonSimpleFormat.json(), JacksonSimpleFormat.json());
    }

    public static <E extends Event> JsonSerializer<E, JsonNode, JsonNode> ofJsonCtxMeta(fr.maif.eventsourcing.vanilla.format.JacksonEventFormat<?, E> eventFormat) {
        return new JsonSerializer<>(eventFormat.toFormat(), JacksonSimpleFormat.json(), JacksonSimpleFormat.json());
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        stringSerializer.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String topic, EventEnvelope<E, Meta, Context> data) {
        String envelopeString = EventEnvelopeJson.serializeToString(data, eventFormat, metaFormat, contextFormat);
        return stringSerializer.serialize(topic, envelopeString);
    }

    @Override
    public void close() {
        stringSerializer.close();
    }
}
