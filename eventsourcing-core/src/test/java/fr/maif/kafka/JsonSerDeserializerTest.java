package fr.maif.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.TextNode;
import fr.maif.Helpers;
import fr.maif.eventsourcing.EventEnvelope;
import fr.maif.eventsourcing.format.JacksonSimpleFormat;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class JsonSerDeserializerTest {

    @Test
    public void test() {

        Helpers.VikingEventFormat format = new Helpers.VikingEventFormat();
        JsonDeserializer<Helpers.VikingEvent, JsonNode, JsonNode> des = JsonDeserializer.of(format, JacksonSimpleFormat.json(), JacksonSimpleFormat.json());
        JsonSerializer<Helpers.VikingEvent, JsonNode, JsonNode> ser = JsonSerializer.ofJsonCtxMeta(format);

        EventEnvelope<Helpers.VikingEvent, JsonNode, JsonNode> envelope = EventEnvelope.<Helpers.VikingEvent, JsonNode, JsonNode>builder()
                .withId(UUID.randomUUID())
                .withEntityId("1")
                .withSequenceNum(1L)
                .withEventType(Helpers.VikingEvent.VikingCreatedV1.name())
                .withVersion(Helpers.VikingEvent.VikingCreatedV1.version())
                .withTotalMessageInTransaction(1)
                .withNumMessageInTransaction(1)
                .withContext(new TextNode("context"))
                .withMetadata(new TextNode("metadata"))
                .withTransactionId("1")
                .withEvent(new Helpers.VikingEvent.VikingCreated("1", "ragnar"))
                .build();

        byte[] tests = ser.serialize("test", envelope);
        EventEnvelope<Helpers.VikingEvent, JsonNode, JsonNode> serDeEnvelope = des.deserialize("test", tests);

        assertThat(serDeEnvelope).isEqualTo(envelope);


    }


}