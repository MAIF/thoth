package fr.maif.reactor.kafka;

import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public record SenderOptions<K, V>(Map<String, Object> properties, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
    public static <K, V> SenderOptions<K, V> create(Map<String, Object> bootstrapServersConfig) {
        return new SenderOptions<>(bootstrapServersConfig, null, null);
    }

    public SenderOptions<K, V> withKeySerializer(Serializer<K> stringSerializer) {
        return new SenderOptions<>(properties, stringSerializer, valueSerializer);
    }

    public SenderOptions<K,V> withValueSerializer(Serializer<V> valueSerializer) {
        return new SenderOptions<>(properties, keySerializer, valueSerializer);
    }
}
