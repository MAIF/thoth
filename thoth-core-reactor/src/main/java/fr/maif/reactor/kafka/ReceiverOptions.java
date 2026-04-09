package fr.maif.reactor.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.function.Predicate.not;

public record ReceiverOptions<K, V>(Map<String, Object> properties, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer, List<String> subscription) {

    public static <K, V> ReceiverOptions<K, V> create(Map<String, Object> config) {
        return new ReceiverOptions<K, V>(config, null, null, List.of());
    }

    public ReceiverOptions<K, V> withKeyDeserializer(Deserializer<K> keyDeserializer) {
        return new ReceiverOptions<>(properties, keyDeserializer, valueDeserializer, subscription);
    }

    public ReceiverOptions<K, V> withValueDeserializer(Deserializer<V> valueDeserializer) {
        return new ReceiverOptions<>(properties, keyDeserializer, valueDeserializer, subscription);
    }

    public ReceiverOptions<K, V> subscription(List<String> subscription) {
        return new ReceiverOptions<>(properties, keyDeserializer, valueDeserializer, subscription);
    }

    public String bootstrapServers() {
        return (String) properties.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
    }

    public ReceiverOptions<K, V> consumerProperty(String key, Object value) {
        var properties = Stream.concat(this.properties.entrySet().stream().filter(not(e -> e.getKey().equals(key))), Stream.of(Map.entry(key, value))).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        return new ReceiverOptions<>(properties, keyDeserializer, valueDeserializer, subscription);
    }
}
