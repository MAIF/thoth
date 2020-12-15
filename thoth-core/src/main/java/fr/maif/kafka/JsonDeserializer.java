package fr.maif.kafka;

import fr.maif.eventsourcing.Event;
import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.eventsourcing.format.JacksonEventFormat;
import fr.maif.eventsourcing.format.JacksonSimpleFormat;
import fr.maif.json.EventEnvelopeJson;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class JsonDeserializer<E extends Event, Meta, Context> implements Deserializer<EventEnvelope<E, Meta, Context>> {

    private final StringDeserializer stringDeserializer;
    private final JacksonEventFormat<?, E> eventFormat;
    private final JacksonSimpleFormat<Meta> metaFormat;
    private final JacksonSimpleFormat<Context> contextFormat;
    private final BiConsumer<String, Object> onError;
    private final Consumer<EventEnvelope<E, Meta, Context>> onSuccess;

    public JsonDeserializer(JacksonEventFormat<?, E> eventFormat, JacksonSimpleFormat<Meta> metaFormat, JacksonSimpleFormat<Context> contextFormat, BiConsumer<String, Object> onError, Consumer<EventEnvelope<E, Meta, Context>> onSuccess) {
        this.eventFormat = eventFormat;
        this.metaFormat = metaFormat;
        this.contextFormat = contextFormat;
        this.onError = onError;
        this.onSuccess = onSuccess;
        this.stringDeserializer = new StringDeserializer();
    }

    public static <E extends Event, Meta, Context> JsonDeserializer<E, Meta, Context> of(JacksonEventFormat<?, E> eventFormat) {
        return new JsonDeserializer<>(eventFormat, JacksonSimpleFormat.empty(), JacksonSimpleFormat.empty(), (__, ___) -> {}, __ -> {});
    }

    public static <E extends Event, Meta, Context> JsonDeserializer<E, Meta, Context> of(JacksonEventFormat<?, E> eventFormat, JacksonSimpleFormat<Meta> metaFormat, JacksonSimpleFormat<Context> contextFormat, BiConsumer<String, Object> onError, Consumer<EventEnvelope<E, Meta, Context>> onSuccess) {
        return new JsonDeserializer<>(eventFormat, metaFormat, contextFormat, onError, onSuccess);
    }


    public static <E extends Event, Meta, Context> JsonDeserializer<E, Meta, Context> of(JacksonEventFormat<?, E> eventFormat, JacksonSimpleFormat<Meta> metaFormat, JacksonSimpleFormat<Context> contextFormat) {
        return new JsonDeserializer<>(eventFormat, metaFormat, contextFormat, (__, ___) -> {}, __ -> {});
    }


    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        stringDeserializer.configure(configs, isKey);
    }

    @Override
    public void close() {
        stringDeserializer.close();
    }

    @Override
    public EventEnvelope<E, Meta, Context> deserialize(String topic, byte[] data) {
        String event = this.stringDeserializer.deserialize(topic, data);
        return EventEnvelopeJson.deserialize(event, eventFormat, metaFormat, contextFormat, onError, onSuccess);
    }
}
