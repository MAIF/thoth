package fr.maif.json;

import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.SerializationFeature;
import tools.jackson.databind.cfg.DateTimeFeature;
import tools.jackson.databind.json.JsonMapper;

public final class MapperSingleton {
    public final static ObjectMapper mapper = buildMapper();

    // Prevent instantiation
    private MapperSingleton() { }

    public static ObjectMapper getInstance() {
        return mapper;
    }

    private static ObjectMapper buildMapper() {
        ObjectMapper mapper = JsonMapper.builder()
                .configure(DateTimeFeature.WRITE_DATES_AS_TIMESTAMPS, false)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
                .build();
        return mapper;
    }
}
