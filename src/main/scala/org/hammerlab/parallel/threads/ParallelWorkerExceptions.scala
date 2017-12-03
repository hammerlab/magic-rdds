package org.hammerlab.parallel.threads

import hammerlab.indent.implicits.tab
import hammerlab.print._
import hammerlab.show._
import org.hammerlab.exception.Error
import org.hammerlab.parallel.threads.ParallelWorkerExceptions.lines

case class ParallelWorkerExceptions[T](exceptions: Seq[ParallelWorkerException[T]])
  extends RuntimeException(
    exceptions.showLines
  )

object ParallelWorkerExceptions {
  implicit def lines[T]: ToLines[Seq[ParallelWorkerException[T]]] =
    new Print(_: Seq[ParallelWorkerException[T]]) {
      write(
        s"${t.length} uncaught exceptions thrown in parallel worker threads:"
      )
      ind {
        for {
          ParallelWorkerException(_, _, exception) ← t
        } {
          write(Error(exception))
        }
      }
    }
}
