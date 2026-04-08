package fr.maif.reactor.kafka;

import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public record SenderOptions<K, V>(Map<String, Object> properties, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
}
