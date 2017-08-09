package org.hammerlab.magic.rdd

import org.apache.spark.rdd.{ RDD, UnionRDD }
import org.hammerlab.magic.rdd.cache.MultiRDDCache

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Exposes a `.size` method to [[RDD]]s that mimics [[RDD.count]], but caches the result.
 *
 * It also exposes `.sizes` and `.total` on [[Seq[RDD]]]s and 2- to 4-tuples which computes the constituent [[RDD]]s'
 * sizes (per above) in one Spark job.
 *
 * Additionally, all the above APIs optimize computations on [[UnionRDD]]s by computing their component [[RDD]]s' sizes
 * (again in just one job, along with any other RDDs being operated on) and caching those as well as the [[UnionRDD]]'s
 * total.
 *
 * Cached-size info is keyed by a [[org.apache.spark.SparkContext]], as well as each RDD's ID, for robustness in apps
 * that stop their [[org.apache.spark.SparkContext]] and then resume with a new one; this is especially useful for
 * testing.
 *
 * Usage:
 * {{{
 * import org.hammerlab.magic.rdd.size._
 * val rdd1 = sc.parallelize(0 until 4)
 * val rdd2 = sc.parallelize("a" :: "b" :: Nil)
 * rdd1.size           // 4; runs one job
 * (rdd1, rdd2).sizes  // (4, 2); runs one job
 * (rdd1, rdd2).total  // 6; runs no jobs
 * rdd2.size           // 2; runs no jobs
 * }}}
 */
package object size extends MultiRDDCache[Any, Long] {

  /**
   * Compute multiple [[RDD]]s' sizes with a single Spark job; cache and return these sizes separately.
   *
   * Along the way, this method will construct a [[UnionRDD]] from all non-cached, non-Union [[RDD]]s.
   */
  override def compute(rdds: Seq[RDD[Any]]): Seq[Long] = {

    val (nonCachedUnionRDDs, nonCachedLeafRDDs) = findNonCachedUnionAndLeafRDDs(rdds)

    computeRDDSizes(nonCachedLeafRDDs)

    // Backfill sizes for UnionRDDs that we didn't compute directly, using the already-computed sizes of their component
    // RDDs.
    for {
      unionRDD ← nonCachedUnionRDDs
    } {
      update(unionRDD, unionRDD.rdds.map(rdd ⇒ apply(rdd)).sum)
    }

    // At this point all RDDs' sizes should have been computed and cached; return them.
    rdds.map(apply)
  }

  /**
   * Traverse the input RDDs (and all descendents if any are UnionRDDs) to obtain all non-cached union and "leaf"
   * (non-union) RDDs.
   *
   * @param rdds a sequence of RDDs, each of which will fall in to one of three categories:
   *               - already cached
   *               - non-cached UnionRDD
   *               - other: (non-cached "leaf" RDD)
   * @return an iterator through all non-cached UnionRDDs in the input RDDs' descendent tree, as well as a sequence of
   *         non-cached non-union ("leaf") RDDs from the same.
   */
  private def findNonCachedUnionAndLeafRDDs(rdds: Seq[RDD[_]]): (Iterator[UnionRDD[_]], Seq[RDD[_]]) = {
    val nonCachedUnionRDDs = ArrayBuffer[UnionRDD[_]]()
    val nonCachedLeafRDDs = ArrayBuffer[RDD[_]]()

    val rddQueue: mutable.Queue[RDD[_]] = mutable.Queue(rdds: _*)

    while (rddQueue.nonEmpty) {
      rddQueue.dequeue() match {

        case cachedRDD: RDD[_] if contains(cachedRDD) ⇒
        // Skip already-computed RDDs.

        case unionRDD: UnionRDD[_] ⇒
          nonCachedUnionRDDs += unionRDD
          unionRDD.rdds.foreach(rddQueue.enqueue(_))

        case rdd: RDD[_] ⇒
          nonCachedLeafRDDs += rdd
      }
    }

    (
      // We encode the iteration order through the UnionRDDs so that each one is processed only once its descendents
      // have been processed.
      nonCachedUnionRDDs.reverseIterator,
      nonCachedLeafRDDs
    )
  }

  /**
   * Compute and cache the sizes of the input RDDs.
   *
   * They should not already exist in the cache, and should not be UnionRDDs.
   */
  private def computeRDDSizes(rdds: Seq[RDD[Any]]): Unit =
    rdds
      .headOption
      .map(_.sparkContext)
      .foreach {
        sc ⇒

          val union = new UnionRDD(sc, rdds)

          // "Map" (really just pairs) from RDD ID to the range which its partitions occupy within the UnionRDD above.
          val partitionRangesByRDD: Seq[(RDD[_], Range)] =
            rdds
              .scanLeft((null: RDD[Any], 0)) {
                case ((lastRDD, offset), curRDD) ⇒
                  val lastRDDPartitions = Option(lastRDD).map(_.getNumPartitions).getOrElse(0)
                  (curRDD: RDD[Any]) → (offset + lastRDDPartitions)
              }
              .drop(1)
              .map {
                case (rdd, offset) ⇒
                  rdd → (offset until offset + rdd.getNumPartitions)
              }

          // Compute each partition's size in the UnionRDD.
          val partitionSizes =
            union
              .mapPartitionsWithIndex(
                (index, iterator) ⇒ Iterator(index → iterator.size),
                preservesPartitioning = true
              )
              .collectAsMap

          // For each RDD, infer its size from the partition-sizes computed above.
          for {
            (rdd, partitionsRange) ← partitionRangesByRDD
            rddSize = partitionsRange.map(partitionSizes(_)).sum
          } {
            update(rdd, rddSize)
          }
      }

  // == Implicit API ==
  implicit class SingleRDDSize(rdd: RDD[_]) {
    def size: Long = apply(rdd)
  }

  implicit class MultiRDDSize(rdds: Seq[RDD[_]]) {
    private lazy val scOpt = rdds.headOption.map(_.sparkContext)

    def total: Long =
      scOpt match {
        case Some(sc) ⇒ apply(rdds).sum
        case None ⇒ 0
      }

    def sizes: Seq[Long] =
      scOpt match {
        case Some(sc) ⇒ apply(rdds)
        case None ⇒ Nil
      }
  }

  // Small wrapper used in Tuple*RDD classes below.
  class HasMultiRDDSize(rdds: RDD[_]*) {
    protected val multiRDDSize = new MultiRDDSize(rdds)
    def total: Long = multiRDDSize.total
  }

  implicit class Tuple2RDDSize(t: (RDD[_], RDD[_])) extends HasMultiRDDSize(t._1, t._2) {
    def sizes: (Long, Long) = {
      val s = multiRDDSize.sizes
      (s(0), s(1))
    }
  }

  implicit class Tuple3RDDSize(t: (RDD[_], RDD[_], RDD[_])) extends HasMultiRDDSize(t._1, t._2, t._3) {
    def sizes: (Long, Long, Long) = {
      val s = multiRDDSize.sizes
      (s(0), s(1), s(2))
    }
  }

  implicit class Tuple4RDDSize(t: (RDD[_], RDD[_], RDD[_], RDD[_])) extends HasMultiRDDSize(t._1, t._2, t._3, t._4) {
    def sizes: (Long, Long, Long, Long) = {
      val s = multiRDDSize.sizes
      (s(0), s(1), s(2), s(3))
    }
  }

  private implicit def toRDDAny(rdd: RDD[_]): RDD[Any] = rdd.asInstanceOf[RDD[Any]]
  private implicit def toRDDsAny(rdds: Seq[RDD[_]]): Seq[RDD[Any]] = rdds.map(toRDDAny)
}
