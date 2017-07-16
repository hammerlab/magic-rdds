package org.hammerlab.parallel.spark

import org.hammerlab.parallel
import org.hammerlab.iterator.FinishingIterator._

import scala.reflect.ClassTag

/**
 * Parallel-map over an [[Iterable]] using Spark
 */
case class Parallelizer[T: ClassTag](input: Iterable[T])(
    implicit
    config: Config
) extends parallel.Parallelizer[T] {


  override def parallelMap[Ctx, U: ClassTag](init: ⇒ Ctx,
                                             fn: (Ctx, T) ⇒ U,
                                             finish: (Ctx) ⇒ Unit): Array[U] = {
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
        .mapPartitions {
          elems ⇒
            val ctx = init
            elems
              .map(
                elem ⇒
                  fn(ctx, elem)
              )
              .finish(
                finish(ctx)
              )
        }
        .collect()

  }

}

