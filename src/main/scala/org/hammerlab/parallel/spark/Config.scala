package org.hammerlab.parallel.spark

import org.hammerlab.parallel
import org.apache.spark.SparkContext

import scala.reflect.ClassTag

/**
 * Configuration for parallel-mapping over collections using Spark.
 */
class Config(val sc: SparkContext,
             val partitioningStrategy: PartitioningStrategy)
  extends parallel.Config {
  override def make[T: ClassTag, From](before: From)(
      implicit toIterable: From ⇒ Iterable[T]
  ): Parallelizer[T] =
    Parallelizer(
      before
    )(
      implicitly[ClassTag[T]],
      this
    )
}

object Config {
  def apply()(
      implicit
      sc: SparkContext,
      strategy: PartitioningStrategy
  ): Config =
    new Config(sc, strategy)
}

