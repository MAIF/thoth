package fr.maif.kafka.consumer;

import lombok.ToString;

public interface Status {

    Started started = new Started();

    Starting starting = new Starting();

    Failed failed = new Failed();

    Stopping stopping = new Stopping();

    Stopped stopped = new Stopped();

    static Status fromString(String status) {
        switch(status) {
            case "Started":
                return started;
            case "Starting":
                return starting;
            case "Failed":
                return failed;
            case "Stopping":
                return stopping;
            case "Stopped":
                return stopped;
            default:
                return stopped;
        }
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
