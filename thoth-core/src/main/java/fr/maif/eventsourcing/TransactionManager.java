package fr.maif.eventsourcing;

import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.impl.TimeBasedGenerator;
import io.vavr.Tuple0;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.Value;
import io.vavr.collection.List;
import io.vavr.concurrent.Future;
import io.vavr.control.Either;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

public interface TransactionManager<TxCtx> {

    TimeBasedGenerator UUIDgen = Generators.timeBasedGenerator();

    <T> Future<T> withTransaction(Function<TxCtx, Future<T>> callBack);

    default String transactionId() {
        return UUIDgen.generate().toString();
    }

    class InTransactionResult<T> {

        public final T results;
        public final Supplier<Future<Tuple0>> postTransactionProcess;

        public InTransactionResult(T results, Supplier<Future<Tuple0>> postTransactionProcess) {
            this.results = results;
            this.postTransactionProcess = postTransactionProcess;
        }

        public Future<T> postTransaction() {
            return postTransactionProcess.get().map(__ -> this.results);
        }

        public <V> InTransactionResult<V> map(Function<T, V> func) {
            return new InTransactionResult<>(func.apply(this.results), this.postTransactionProcess);
        }

        public <V, R> InTransactionResult<R> and(InTransactionResult<V> other, BiFunction<T, V, R> combine) {
            return this.and(other).map(v -> combine.apply(v._1, v._2));
        }

        public <V> InTransactionResult<Tuple2<T, V>> and(InTransactionResult<V> other) {
            Tuple2<T, V> tuple = Tuple.of(this.results, other.results);
            return new InTransactionResult<>(
                    tuple,
                    () -> this.postTransactionProcess.get()
                            .flatMap(r -> other.postTransactionProcess.get())
            );
        }
    }
}
