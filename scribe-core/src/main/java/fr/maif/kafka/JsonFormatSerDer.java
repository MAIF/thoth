package fr.maif.kafka;

import fr.maif.eventsourcing.Event;
import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.json.Json;
import fr.maif.json.JsonFormat;
import io.vavr.control.Try;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class JsonFormatSerDer<E extends Event, Meta, Context> implements Deserializer<EventEnvelope<E, Meta, Context>>, Serializer<EventEnvelope<E, Meta, Context>> {

    private final StringSerializer stringSerializer;
    private final StringDeserializer stringDeserializer;
    private final JsonFormat<EventEnvelope<E, Meta, Context>> eventFormat;
    private final BiConsumer<String, Object> onError;
    private final Consumer<EventEnvelope<E, Meta, Context>> onSuccess;

    public JsonFormatSerDer(JsonFormat<EventEnvelope<E, Meta, Context>> eventFormat,
                            BiConsumer<String, Object> onError,
                            Consumer<EventEnvelope<E, Meta, Context>> onSuccess) {
        this.onError = onError;
        this.onSuccess = onSuccess;
        this.stringDeserializer = new StringDeserializer();
        this.stringSerializer = new StringSerializer();
        this.eventFormat = eventFormat;
    }

    public static <E extends Event, Meta, Context> JsonFormatSerDer<E, Meta, Context> of(JsonFormat<EventEnvelope<E, Meta, Context>> eventRead) {
        return new JsonFormatSerDer<>(eventRead, (__, ___) -> {}, __ -> {});
    }

    public static <E extends Event, Meta, Context> JsonFormatSerDer<E, Meta, Context> of(JsonFormat<EventEnvelope<E, Meta, Context>> eventRead, BiConsumer<String, Object> onError, Consumer<EventEnvelope<E, Meta, Context>> onSuccess) {
        return new JsonFormatSerDer<>(eventRead, onError, onSuccess);
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
        return Try.of(() -> Json.parse(event))
                    .map(json -> eventFormat
                        .read(json)
                        .fold(e -> {
                            onError.accept(event, e);
                            return null;
                        }, ok -> {
                            onSuccess.accept(ok);
                            return ok;
                        })
                    )
                    .getOrElseGet(e -> {
                        onError.accept(event, e);
                        return null;
                    });
    }

    @Override
    public byte[] serialize(String topic, EventEnvelope<E, Meta, Context> data) {
        return stringSerializer.serialize(topic, Json.stringify(eventFormat.write(data)));
    }
}
