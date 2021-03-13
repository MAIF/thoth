# Technical considerations

We made some opinionated choices on Thoth return types/

* The vavr `Future` is used for async call (java `CompletionStage` is not user-friendly).

* The akka stream `Source` is used for stream processing.

* The vavr `Either` is used to handle business errors. The idea is to have three channels:
    * it's ok: `Future(Right("Result"))`
    * it's an error: `Future(Left("Bad request"))`
    * it's a failure: `Future.failed(CrashedException("Crap!"))`

* `io.vavr.Tuple0` is used instead of `void` so everything can be an expression:
    ```java
    Tuple0 sideEffect() {
        println("I have done a side effect");
        return Tuple.empty();
    }
    ```