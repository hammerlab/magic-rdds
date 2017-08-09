package org.hammerlab.parallel.threads

import java.lang.Runtime.getRuntime

import org.hammerlab.parallel

import scala.reflect.ClassTag

case class Config(numThreads: Int)
  extends parallel.Config {
  override def make[T: ClassTag, From](before: From)(
      implicit toIterable: From ⇒ Iterable[T]
  ): Parallelizer[T] =
    Parallelizer(before)(this)
}

object Config {
  implicit def default =
    Config(
      getRuntime()
        .availableProcessors() * 4
    )
}

