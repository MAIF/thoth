package fr.maif.config;

import io.vavr.control.Option;

public class Configs {

    public static Option<String> getOptionalString(com.typesafe.config.Config config, String key) {
        if (config.hasPath(key)) {
            return Option.of(config.getString(key));
        } else {
            return Option.none();
        }
    }
}
