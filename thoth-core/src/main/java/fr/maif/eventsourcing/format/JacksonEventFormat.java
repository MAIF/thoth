package fr.maif.eventsourcing.format;

import tools.jackson.databind.JsonNode;

public interface JacksonEventFormat<Err, E> extends EventFormat<Err, E, JsonNode> {
}
