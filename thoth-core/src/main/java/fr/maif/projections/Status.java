package fr.maif.projections;

import static io.vavr.API.$;
import static io.vavr.API.Case;
import static io.vavr.API.Match;

public interface Status {

    Started started = new Started();

    Starting starting = new Starting();

    Failed failed = new Failed();

    Stopping stopping = new Stopping();

    Stopped stopped = new Stopped();

    static Status fromString(String status) {
        return Match(status).of(
                Case($("Started"), started),
                Case($("Starting"), starting),
                Case($("Failed"), failed),
                Case($("Stopping"), stopping),
                Case($("Stopped"), stopped)
        );
    }

    String name();

    class Started implements Status {
        @Override
        public String name() {
            return "Started";
        }

        @Override
        public String toString() {
            return "Started{}";
        }
    }

    class Starting implements Status {
        @Override
        public String name() {
            return "Starting";
        }

        @Override
        public String toString() {
            return "Starting{}";
        }
    }

    class Failed implements Status {
        @Override
        public String name() {
            return "Failed";
        }

        @Override
        public String toString() {
            return "Failed{}";
        }
    }

    class Stopped implements Status {
        @Override
        public String name() {
            return "Stopped";
        }

        @Override
        public String toString() {
            return "Stopped{}";
        }
    }

    class Stopping implements Status {
        @Override
        public String name() {
            return "Stopping";
        }

        @Override
        public String toString() {
            return "Stopping{}";
        }
    }

}
