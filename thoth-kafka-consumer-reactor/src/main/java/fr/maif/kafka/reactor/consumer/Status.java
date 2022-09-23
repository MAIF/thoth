package fr.maif.kafka.reactor.consumer;


public enum Status {
    Started("Started"),
    Starting("Starting"),
    Stopped("Stopped"),
    Stopping("Stopping"),
    Failed("Failed");

    public final String name;

    Status(String name) {
        this.name = name;
    }

    static Status fromString(String status) {
        switch(status) {
            case "Started":
                return Started;
            case "Starting":
                return Starting;
            case "Failed":
                return Failed;
            case "Stopping":
                return Stopping;
            case "Stopped":
                return Stopped;
            default:
                return Stopped;
        }
    }
}
