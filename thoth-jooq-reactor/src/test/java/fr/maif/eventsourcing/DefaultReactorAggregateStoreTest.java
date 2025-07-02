package fr.maif.eventsourcing;

import io.vavr.Tuple;
import io.vavr.Tuple0;
import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.control.Option;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.function.Function;

import static fr.maif.eventsourcing.AbstractPostgresEventStoreTest.eventEnvelope;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DefaultReactorAggregateStoreTest {

    private ReactorEventStore<Tuple0, CountEvent, Tuple0, Tuple0> eventStore;

    record CountState(String id, Long sequenceNum, Integer count) implements State<CountState> {
        @Override
        public String entityId() {
            return id;
        }

        @Override
        public Long sequenceNum() {
            return sequenceNum;
        }

        @Override
        public CountState withSequenceNum(Long sequenceNum) {
            return new CountState(id, sequenceNum, count);
        }
    }

    sealed interface CountEvent extends Event {
        record AddedEvent(String id, Integer add) implements CountEvent {
            @Override
            public Type<?> type() {
                return Type.create(AddedEvent.class, 1L);
            }

            @Override
            public String entityId() {
                return id;
            }
        }
    }

    EventHandler<CountState, CountEvent> eventHandler = (EventHandler<CountState, CountEvent>) (state, events) -> {
        if( events instanceof CountEvent.AddedEvent addedEvent) {
            return state.map(s -> new CountState(s.id, s.sequenceNum, s.count + addedEvent.add)).orElse(
                    Option.of(new CountState(addedEvent.id(), 0L, addedEvent.add)));
        } else {
            return state;
        }
    };

    DefaultReactorAggregateStore<CountState, CountEvent, Tuple0, Tuple0, Tuple0> defaultReactorAggregateStore;

    @BeforeEach
    public void setUp() {
        eventStore = mock(ReactorEventStore.class);
        defaultReactorAggregateStore = new DefaultReactorAggregateStore<>(
                eventStore, eventHandler, new ReactorTransactionManager<Tuple0>() {
            @Override
            public <T> Mono<T> withTransaction(Function<Tuple0, Mono<T>> callBack) {
                return callBack.apply(Tuple.empty());
            }
        }, ReadConcurrencyStrategy.NO_STRATEGY) {
            @Override
            public Mono<List<CountState>> getSnapshots(Tuple0 transactionContext, List<String> strings) {
                return Mono.just(strings.flatMap(id -> {
                   if (id.equals("5")) {
                       return Option.of(new CountState(id, 0L, 50));
                   } else {
                       return Option.<CountState>none();
                   }
                }));
            }
        };
    }

    @Test
    void loadEventToAggregates() {
        List<Integer> idsInt = List.range(1, 10);
        List<String> ids = idsInt.map(String::valueOf);

        when(eventStore.loadEventsByQuery(any(), any())).thenReturn(Flux.fromIterable(idsInt.flatMap(id ->
                List.range(0, id).map(add ->
                        new CountEvent.AddedEvent(String.valueOf(id), add)
                )
        )).map(evt -> eventEnvelope(0L, evt, LocalDateTime.now())));

        Map<String, Option<CountState>> result = defaultReactorAggregateStore.getAggregates(Tuple.empty(), ids).block();
        assertThat(result).isEqualTo(HashMap.ofEntries(List.of(
            new CountState("1", 0L, 0),
            new CountState("2", 0L, 1),
            new CountState("3", 0L, 3),
            new CountState("4", 0L, 6),
            new CountState("5", 0L, 60),
            new CountState("6", 0L, 15),
            new CountState("7", 0L, 21),
            new CountState("8", 0L, 28),
            new CountState("9", 0L, 36)
        ).map(s -> Tuple.of(s.id, Option.of(s)))));

    }


}