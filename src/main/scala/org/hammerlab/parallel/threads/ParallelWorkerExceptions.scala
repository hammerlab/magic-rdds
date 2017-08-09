package org.hammerlab.parallel.threads

import org.hammerlab.exception.Error
import org.hammerlab.parallel.threads.ParallelWorkerExceptions.lines

case class ParallelWorkerExceptions[T](exceptions: Seq[ParallelWorkerException[T]])
  extends RuntimeException(
    lines(exceptions)
      .mkString("\n")
  )

object ParallelWorkerExceptions {
  def lines(exceptions: Seq[ParallelWorkerException[_]]): List[String] =
    s"${exceptions.length} uncaught exceptions thrown in parallel worker threads:" ::
      (
        for {
          (ParallelWorkerException(_, _, exception)) ← exceptions.toList
          line ← Error(exception).lines()
        } yield
          s"\t$line"
      )
}
