package fr.maif.concurrent

import java.util.Collections
import java.util.concurrent.{AbstractExecutorService, TimeUnit}
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}

object ExecutionContextBridge {
  def apply(ec: ExecutionContext): ExecutionContextExecutorService =
    ec match {
      case null                                  => throw null
      case eces: ExecutionContextExecutorService => eces
      case other                                 =>
        new AbstractExecutorService with ExecutionContextExecutorService {
          override def prepare(): ExecutionContext                             = other
          override def isShutdown                                              = false
          override def isTerminated                                            = false
          override def shutdown()                                              = ()
          override def shutdownNow()                                           = Collections.emptyList[Runnable]
          override def execute(runnable: Runnable): scala.Unit                 = other execute runnable
          override def reportFailure(t: Throwable): scala.Unit                 = other reportFailure t
          override def awaitTermination(length: Long, unit: TimeUnit): Boolean = false
        }
    }
}
