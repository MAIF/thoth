package fr.maif.thoth.sample.configuration;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.kafka.JsonDeserializer;
import fr.maif.thoth.sample.events.BankEvent;
import fr.maif.thoth.sample.events.BankEventFormat;
import io.vavr.Tuple0;

@Configuration
@EnableKafka
public class SpringKafkaConsumptionConfig {
    @Bean
    public ConsumerFactory<String, EventEnvelope<BankEvent, Tuple0, Tuple0>> consumerFactory(
            @Value("${kafka.port}") int port,
            @Value("${kafka.host}") String host,
            BankEventFormat eventFormat) {
        Map<String, Object> props = new HashMap<>();
        props.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                host + ":" + port);
        props.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                "stats");

        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), JsonDeserializer.of(eventFormat));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, EventEnvelope<BankEvent, Tuple0, Tuple0>>
    kafkaListenerContainerFactory(ConsumerFactory<String, EventEnvelope<BankEvent, Tuple0, Tuple0>> consumerFactory) {

        ConcurrentKafkaListenerContainerFactory<String, EventEnvelope<BankEvent, Tuple0, Tuple0>> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        return factory;
    }
}
