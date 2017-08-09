package org.hammerlab.parallel.threads

case class ParallelWorkerException[T](elem: T,
                                      idx: Int,
                                      exception: Throwable)
  extends RuntimeException(
    s"Index $idx, element $elem:",
    exception
  )
