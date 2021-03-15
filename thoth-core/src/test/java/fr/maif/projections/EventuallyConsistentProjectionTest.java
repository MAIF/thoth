package fr.maif.projections;

import akka.actor.ActorSystem;
import akka.kafka.testkit.javadsl.TestcontainersKafkaTest;
import akka.stream.Materializer;
import akka.testkit.javadsl.TestKit;
import com.fasterxml.jackson.databind.JsonNode;
import fr.maif.Helpers;
import fr.maif.Helpers.VikingEvent;
import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.json.Json;
import fr.maif.json.JsonFormat;
import fr.maif.projections.EventuallyConsistentProjection.Config;
import io.vavr.Tuple;
import io.vavr.Tuple0;
import io.vavr.concurrent.Future;
import io.vavr.control.Option;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static fr.maif.Helpers.VikingEvent.VikingCreatedV1;
import static fr.maif.Helpers.VikingEvent.VikingUpdatedV1;
import static io.vavr.API.Case;
import static io.vavr.API.Match;
import static io.vavr.API.println;
import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class EventuallyConsistentProjectionTest extends TestcontainersKafkaTest {

    private static JsonFormat<EventEnvelope<VikingEvent, Tuple0, Tuple0>> vikingEventJsonFormat = new Helpers.VikingEventJsonFormat();
    private static final ActorSystem system = ActorSystem.create("test");

    public EventuallyConsistentProjectionTest() {
        super(system, Materializer.createMaterializer(system));
    }

    @Test
    @SneakyThrows
    void consumer() {

        String topic = createTopic();
        String groupId = "test-group-id";

        AtomicReference<String> names = new AtomicReference<>("");

        EventuallyConsistentProjection.create(
                system, "test", Config.create(topic, groupId, bootstrapServers()),
                new Helpers.VikingEventFormat(),
                event -> {
                    Option<String> name = Match(event.event).option(
                            Case(VikingCreatedV1.pattern(), e -> e.name),
                            Case(VikingUpdatedV1.pattern(), e -> e.name)
                    );
                    name.forEach(n -> names.set(names.get() + " " + n));
                    return Future.successful(Tuple.empty());
                }
        );
        Thread.sleep(1000);

        resultOf(produceString(topic, stringEvent(new VikingEvent.VikingCreated("1", "Lodbrock"))));
        resultOf(produceString(topic, stringEvent(new VikingEvent.VikingCreated("2", "Lagerta"))));
        resultOf(produceString(topic, stringEvent(new VikingEvent.VikingUpdated("1", "Lodbrok"))));

        Thread.sleep(2000);

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


    @AfterAll
    void shutdownActorSystem() {
        TestKit.shutdownActorSystem(system);
    }

}