package fr.maif.reactor.projections;

import com.fasterxml.jackson.databind.JsonNode;
import fr.maif.Helpers;
import fr.maif.Helpers.VikingEvent;
import fr.maif.concurrent.CompletionStages;
import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.json.Json;
import fr.maif.json.JsonFormat;
import fr.maif.reactor.KafkaHelper;
import fr.maif.reactor.projections.EventuallyConsistentProjection.Config;
import io.vavr.API;
import io.vavr.Tuple0;
import io.vavr.control.Option;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static io.vavr.API.Match;
import static io.vavr.API.println;
import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class EventuallyConsistentProjectionTest extends KafkaHelper {

    private static JsonFormat<EventEnvelope<VikingEvent, Tuple0, Tuple0>> vikingEventJsonFormat = new Helpers.VikingEventJsonFormat();

    @Test
    void consumer() throws Exception {

        String topic = createTopic();
        String groupId = "test-group-id";

        AtomicReference<String> names = new AtomicReference<>("");

        EventuallyConsistentProjection.simpleHandler(
                "test",
                Config.create(topic, groupId, bootstrapServers()),
                new Helpers.VikingEventFormat(),
                event -> {
                    Option<String> name = Match(event.event).option(
                            API.Case(VikingEvent.VikingCreatedV1.pattern(), e -> e.name),
                            API.Case(VikingEvent.VikingUpdatedV1.pattern(), e -> e.name)
                    );
                    name.forEach(n -> names.set(names.get() + " " + n));
                    return CompletionStages.empty();
                }
        );
        Thread.sleep(3000);

        produceString(topic, stringEvent(new VikingEvent.VikingCreated("1", "Lodbrock")));
        produceString(topic, stringEvent(new VikingEvent.VikingCreated("2", "Lagerta")));
        produceString(topic, stringEvent(new VikingEvent.VikingUpdated("1", "Lodbrok")));

        Thread.sleep(1000);

        String actual = names.get();
        println(actual);
        assertThat(actual).isEqualTo(" Lodbrock Lagerta Lodbrok");

    }

    private String stringEvent(VikingEvent event) {
        return Json.stringify(jsonEvent(event));
    }

    private JsonNode jsonEvent(VikingEvent event) {
        JsonNode jsonNode = Json.toJson(eventEnvelope(event), vikingEventJsonFormat);
        println(jsonNode);
        return jsonNode;
    }

    private EventEnvelope<VikingEvent, Tuple0, Tuple0> eventEnvelope(VikingEvent event) {
        return EventEnvelope.<VikingEvent, Tuple0, Tuple0>builder()
                .withId(UUID.randomUUID())
                .withEntityId(event.entityId())
                .withVersion(1L)
                .withEventType(event.getClass().getSimpleName())
                .withEvent(event)
                .withEmissionDate(LocalDateTime.now())
                .build();
    }

}