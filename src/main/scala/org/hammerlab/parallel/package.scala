package org.hammerlab

import java.lang.Runtime.getRuntime

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
    def parallel[U: ClassTag](fn: T ⇒ U): Array[U] =
      parallelMap(
        Unit,
        (_: Unit.type, t) ⇒ fn(t),
        (_: Unit.type) ⇒ {}
      )

    def parallel[U: ClassTag](init: ⇒ Unit, fn: T ⇒ U, finish: ⇒ Unit): Array[U] =
      parallelMap(
        init,
        (_: Unit, t) ⇒ fn(t),
        (_: Unit) ⇒ finish
      )

    def parallelMap[Ctx, U: ClassTag](init: ⇒ Ctx, fn: (Ctx, T) ⇒ U, finish: Ctx ⇒ Unit): Array[U]
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
   * enables [[Parallelizer.parallelMap]]'ing on collections.
   *
   * @param input collection to [[Parallelizer.parallelMap]] over
   * @param config implicit [[Config]] specifying how to parallel-map
   * @tparam T [[Input]] element type
   * @tparam Input collection type, implicitly convertible to [[Iterable]]
   * @tparam Result [[Parallelizer]] to generate
   * @return instance of [[Result]] type, exposing [[Parallelizer.parallelMap]] method on provided `input`
   */
  implicit def makeParallelizer[T: ClassTag, Input, Result[E] <: Parallelizer[E]](input: Input)(
      implicit
      toIterable: Input ⇒ Iterable[T],
      config: Config
  ): Parallelizer[T] =
    config.make[T, Input](input)

  def defaultNumThreads: Int =
    getRuntime().availableProcessors() * 4

  def Threads(numThreads: Int = defaultNumThreads) =
    threads.Config(numThreads)

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
