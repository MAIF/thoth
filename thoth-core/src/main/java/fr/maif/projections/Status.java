package fr.maif.projections;

import lombok.ToString;

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

    @ToString
    class Started implements Status {
        @Override
        public String name() {
            return "Started";
        }
    }

    @ToString
    class Starting implements Status {
        @Override
        public String name() {
            return "Starting";
        }
    }

    @ToString
    class Failed implements Status {
        @Override
        public String name() {
            return "Failed";
        }
    }

    @ToString
    class Stopped implements Status {
        @Override
        public String name() {
            return "Stopped";
        }
    }

    @ToString
    class Stopping implements Status {
        @Override
        public String name() {
            return "Stopping";
        }
    }

}
