package fr.maif.reactor.kafka;

import org.apache.kafka.clients.producer.RecordMetadata;

public record SenderResult<T>(RecordMetadata recordMetadata, Exception exception, T correlationMetadata) {
}
