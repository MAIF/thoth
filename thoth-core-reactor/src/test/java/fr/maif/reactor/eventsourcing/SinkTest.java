package fr.maif.reactor.eventsourcing;

import fr.maif.reactor.eventsourcing.ReactorKafkaEventPublisher.ReactorQueue;
import io.vavr.collection.List;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class SinkTest {




    @Test
    public void test() {
        Sinks.Many<String> queue = Sinks.many().replay().latest();
        AtomicBoolean failed = new AtomicBoolean(false);
        AtomicBoolean failedStream = new AtomicBoolean(false);
        CountDownLatch countDownLatch = new CountDownLatch(1);

        Mono.defer(() -> {
            if (failed.getAndSet(true)) {
                return Mono.just("");
            } else {
                return Mono.error(new RuntimeException("Oups"));
            }
        }).flatMap(__ ->
                queue.asFlux()
//                        .log()
                        .concatMap(element -> {
                            if (failedStream.getAndSet(true)) {
                                System.out.println(element);
                                return Mono.just(element);
                            } else {
                                return Mono.error(new RuntimeException("Oups stream"));
                            }
                        })
                        .ignoreElements()
        )
        .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1))
                .transientErrors(true)
                .maxBackoff(Duration.ofSeconds(5))
                .doBeforeRetry(ctx -> {
                    System.out.println("Error handling events for topic %s retrying for the %s time".formatted("test", ctx.totalRetries() + 1));
                    ctx.failure().printStackTrace();
                })
        ).subscribe();


        Flux.range(0, 20)
                .delayElements(Duration.ofSeconds(1))
                .subscribe(n -> System.out.println(queue.tryEmitNext(n.toString())));

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }
    @Test
    public void test2() {
        ReactorQueue<String> queue = new ReactorQueue<>(10000);
        AtomicBoolean failed = new AtomicBoolean(true);
        AtomicBoolean failedStream = new AtomicBoolean(true);
        CountDownLatch countDownLatch = new CountDownLatch(1);

        Mono.defer(() -> {
            if (failed.getAndSet(true)) {
                return Mono.just("");
            } else {
                return Mono.error(new RuntimeException("Oups"));
            }
        }).flatMap(__ ->
                queue.asFlux()
//                        .log()
                        .concatMap(element -> {
                            if (failedStream.getAndSet(true)) {
                                System.out.println(element);
                                return Mono.just(element);
                            } else {
                                return Mono.error(new RuntimeException("Oups stream"));
                            }
                        })
                        .ignoreElements()
        )
        .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1))
                .transientErrors(true)
                .maxBackoff(Duration.ofSeconds(5))
                .doBeforeRetry(ctx -> {
                    System.out.println("Error handling events for topic %s retrying for the %s time".formatted("test", ctx.totalRetries() + 1));
                    ctx.failure().printStackTrace();
                })
        ).subscribe();


        Flux.range(0, 20)
//                .delayElements(Duration.ofSeconds(1))
                .doOnNext(n -> queue.offer(List.of(n.toString())))
                .collectList()
                .block();
        Flux.range(0, 20)
                .delayElements(Duration.ofSeconds(1))
                .doOnNext(n -> queue.offer(List.of(n.toString())))
                .collectList()
                .block();

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }
}
