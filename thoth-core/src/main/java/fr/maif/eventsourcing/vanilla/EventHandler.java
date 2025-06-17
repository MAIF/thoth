package fr.maif.eventsourcing.vanilla;

import io.vavr.control.Option;

import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public interface EventHandler<State, Event> {

    private static <A, B> Collector<A, ?, B> foldLeft(final B init, final BiFunction<? super B, ? super A, ? extends B> f) {
        return Collectors.collectingAndThen(
                Collectors.reducing(Function.<B>identity(), a -> b -> f.apply(b, a), Function::andThen),
                endo -> endo.apply(init)
        );
    }

    Optional<State> applyEvent(Optional<State> state, Event events);

    default Optional<State> deriveState(Optional<State> state, List<Event> events) {
        return events.stream().collect(foldLeft(state, this::applyEvent));
    }

    default fr.maif.eventsourcing.EventHandler<State, Event> toEventHandler() {
        var _this = this;
        return new fr.maif.eventsourcing.EventHandler<>() {
            @Override
            public Option<State> applyEvent(Option<State> state, Event events) {
                return Option.ofOptional(_this.applyEvent(state.toJavaOptional(), events));
            }
        };
    }

    static <State, Event> EventHandler<State, Event> from(fr.maif.eventsourcing.EventHandler<State, Event> handler) {
        return new EventHandler<State, Event>() {
            @Override
            public Optional<State> applyEvent(Optional<State> state, Event events) {
                return handler.applyEvent(Option.ofOptional(state), events)
                        .toJavaOptional();
            }
        };
    }
}
