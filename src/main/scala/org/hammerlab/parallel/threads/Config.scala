package org.hammerlab.parallel.threads

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
  implicit val default = Config(8)
}

