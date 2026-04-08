package fr.maif.reactor.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

public record KafkaSender<K, V>(KafkaProducer<K, V> producer) implements Closeable {

    public static <K, V> KafkaSender<K, V> create(SenderOptions<K, V> senderOptions) {
        Map<String, Object> properties = senderOptions.properties().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        return new KafkaSender<>(new KafkaProducer<>(properties, senderOptions.keySerializer(), senderOptions.valueSerializer()));
    }

    public <T> Flux<SenderResult<T>> send(Publisher<SenderRecord<K, V, T>> message) {
        return Flux.from(message).concatMap(m ->
            Mono.create(sink -> {
                producer.send(m, (metadata, exception) -> {
                    if (exception != null) {
                        sink.error(exception);
                    } else {
                        sink.success(new SenderResult<>(metadata, null, m.correlationMetadata()));
                    }
                });

            })
        );
    }

    @Override
    public void close() throws IOException {
        producer.close();
    }
}
