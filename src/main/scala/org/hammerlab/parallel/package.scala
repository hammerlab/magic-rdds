package org.hammerlab

import org.apache.spark.SparkContext
import org.hammerlab.parallel.spark.PartitioningStrategy

import scala.reflect.ClassTag

/**
 * Interfaces for pluggable parallelization using either Spark or local threads
 */
package object parallel {

  /**
   * Interface for wrapping a collection and exposing a "parallel map" method on it
   */
  trait Parallelizer[+T] {
    def pmap[U: ClassTag](fn: T ⇒ U): Array[U]
  }

  /**
   * Interface for objects that configure [[Parallelizer]]s
   */
  trait Config {
    def make[T: ClassTag, From](before: From)(
        implicit toIterable: From ⇒ Iterable[T]
    ): Parallelizer[T]
  }

  /**
   * Helper-implicit for converting from a [[Config]] to a [[Parallelizer]], so that an implicit [[Config]] in scope
   * enables [[Parallelizer.pmap]]'ing on collections.
   *
   * @param input collection to [[Parallelizer.pmap]] over
   * @param config implicit [[Config]] specifying how to parallel-map
   * @tparam T [[Input]] element type
   * @tparam Input collection type, implicitly convertible to [[Iterable]]
   * @tparam Result [[Parallelizer]] to generate
   * @return instance of [[Result]] type, exposing [[Parallelizer.pmap]] method on provided `input`
   */
  implicit def makeParallelizer[T: ClassTag, Input, Result[E] <: Parallelizer[E]](input: Input)(
      implicit
      toIterable: Input ⇒ Iterable[T],
      config: Config
  ): Parallelizer[T] =
    config.make[T, Input](input)

  def Threads(numThreads: Int) = threads.Config(numThreads)

  def Spark(strategy: PartitioningStrategy)(
      implicit sc: SparkContext
  ): spark.Config =
    spark.Config()(sc, strategy)

  def Spark(implicit
            sc: SparkContext,
            strategy: PartitioningStrategy
           ): spark.Config =
    spark.Config()(sc, strategy)
}
