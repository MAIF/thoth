package fr.maif.eventsourcing.format;

import com.fasterxml.jackson.databind.JsonNode;

public interface JacksonEventFormat<Err, E> extends EventFormat<Err, E, JsonNode> {
}
