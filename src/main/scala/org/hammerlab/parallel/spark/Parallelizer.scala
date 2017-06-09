package org.hammerlab.parallel.spark

import org.hammerlab.parallel

import scala.reflect.ClassTag

/**
 * Parallel-map over an [[Iterable]] using Spark
 */
case class Parallelizer[T: ClassTag](input: Iterable[T])(
    implicit
    config: Config
) extends parallel.Parallelizer[T] {

  def pmap[U: ClassTag](fn: T ⇒ U): Array[U] =
    if (input.isEmpty)
      Array()
    else
      config
        .sc
        .parallelize(
          input.toSeq,
          config
          .partitioningStrategy
          .numPartitions(input.size)
        )
        .map(fn)
        .collect()
}

