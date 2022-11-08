# Technical considerations

We made some opinionated choices on Thoth return types/

* The java `CompletionStage` is used for async call but there is wrappers that exposes `Mono` for spring integration.

* The reactive stream `Publisher` is used for stream processing and akka `Source` and spring `Flux` can be used in modules.

* The vavr `Either` is used to handle business errors. The idea is to have three channels:
    * it's ok: `CompletionStage(Right("Result"))`
    * it's an error: `CompletionStage(Left("Bad request"))`
    * it's a failure: `CompletionStage.failed(CrashedException("Crap!"))`

* `io.vavr.Tuple0` is used instead of `void` so everything can be an expression:
    ```java
    Tuple0 sideEffect() {
        println("I have done a side effect");
        return Tuple.empty();
    }
    ```