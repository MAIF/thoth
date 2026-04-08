package fr.maif.reactor.kafka;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;

public class SenderRecord<K, V, T> extends ProducerRecord<K, V> {
    private final T correlationMetadata;

    public static <K, V, T> SenderRecord<K, V, T> create(ProducerRecord<K, V> record, T correlationMetadata) {
        return new SenderRecord<K, V, T>(record.topic(), record.partition(), record.timestamp(), record.key(), record.value(), correlationMetadata, record.headers());
    }

    public static <K, V, T> SenderRecord<K, V, T> create(String topic, Integer partition, Long timestamp, K key, V value, T correlationMetadata) {
        return new SenderRecord<K, V, T>(topic, partition, timestamp, key, value, correlationMetadata, (Iterable)null);
    }

    private SenderRecord(String topic, Integer partition, Long timestamp, K key, V value, T correlationMetadata, Iterable<Header> headers) {
        super(topic, partition, timestamp, key, value, headers);
        this.correlationMetadata = correlationMetadata;
    }

    public T correlationMetadata() {
        return this.correlationMetadata;
    }
}
