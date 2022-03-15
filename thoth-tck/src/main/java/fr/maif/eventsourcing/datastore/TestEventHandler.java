package fr.maif.eventsourcing.datastore;

import fr.maif.eventsourcing.EventHandler;
import io.vavr.control.Option;

import static io.vavr.API.Case;
import static io.vavr.API.Match;

public class TestEventHandler implements EventHandler<TestState, TestEvent> {
    @Override
    public Option<TestState> applyEvent(Option<TestState> previousState, TestEvent event) {
        return Match(event).of(
                Case(TestEvent.SimpleEventV1.pattern(), evt -> {
                    Integer previousCount = previousState.map(p -> p.count).getOrElse(0);
                    return Option.some(new TestState(event.id, previousCount + 1));
                }),
                Case(TestEvent.DeleteEventV1.pattern(), evt -> Option.none()),
            Case(TestEvent.DecreaseEventV1.pattern(), evt -> {
              final int count = previousState.get().count;
              return Option.some(new TestState(event.id, count - 1));
            })
        );
    }
}
