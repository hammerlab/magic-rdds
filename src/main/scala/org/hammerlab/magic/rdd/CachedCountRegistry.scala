package org.hammerlab.magic.rdd

import org.apache.spark.SparkContext
import org.apache.spark.rdd.{RDD, UnionRDD}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * [[CachedCountRegistry]] adds a `.size` method to [[RDD]]s that mimicks [[RDD.count]], but caches its result.
 *
 * It also exposes `.sizes` and `.total` on [[Seq[RDD]]]s, which compute the constituent [[RDD]]s' sizes (per above) in
 * one Spark job.
 *
 * Additionally, both sets of APIs optimize computations on [[UnionRDD]]s by computing their component [[RDD]]s' sizes
 * and caching those as well as the [[UnionRDD]]'s total.
 *
 * Cached `size` info is keyed by a [[SparkContext]] for robustness in apps that stop their [[SparkContext]] and then
 * resume with a new one; this is especially useful for testing!
 *
 * Usage:
 * {{{
\ * import org.hammerlab.magic.rdd.CachedCountRegistry._
 * val rdd1 = sc.parallelize(0 until 4)
 * val rdd2 = sc.parallelize("a" :: "b" :: Nil)
 * rdd1.size()
 * (rdd1 :: rdd2 :: Nil).sizes()
 * (rdd1 :: rdd2 :: Nil).total()
 * }}}
 */
class CachedCountRegistry private() {
  private val cache = mutable.HashMap[Int, Long]()

  /** Count RDD and cache result for RDD id */
  def cachedCount(rdd: RDD[_]): Long = multiCachedCount(List(rdd))(0)

  /**
   * Compute multiple [[RDD]]s' sizes with a single Spark job; cache and return these sizes separately.
   *
   * Along the way, this method will construct a [[UnionRDD]] from all non-cached, non-Union [[RDD]]s.
   */
  def multiCachedCount(rdds: Seq[RDD[_]]): Seq[Long] = {

    val (nonCachedUnionRDDs, nonCachedLeafRDDs) = findNonCachedUnionAndLeafRDDs(rdds)

    computeRDDSizes(nonCachedLeafRDDs)

    // Backfill sizes for UnionRDDs that we didn't compute directly, using the already-computed sizes of their component
    // RDDs.
    for {
      unionRDD <- nonCachedUnionRDDs
    } {
      cache(unionRDD.id) = unionRDD.rdds.map(rdd => cache(rdd.id)).sum
    }

    // At this point all RDDs' sizes should have been computed and cached; return them.
    rdds.map(rdd => cache(rdd.id))
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

        case cachedRDD: RDD[_]
          if cache.contains(cachedRDD.id) =>
            // Skip already-computed RDDs.

        case unionRDD: UnionRDD[_] =>
          nonCachedUnionRDDs += unionRDD
          unionRDD.rdds.foreach(rddQueue.enqueue(_))

        case rdd: RDD[_] =>
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
  private def computeRDDSizes(rdds: Seq[RDD[_]]): Unit =
    rdds
      .headOption
      .map(_.sparkContext)
      .foreach {
        sc =>

          // UnionRDD needs RDD-types genericized.
          val genericRDDs = rdds.map(_.asInstanceOf[RDD[Any]])

          val union = new UnionRDD(sc, genericRDDs)

          // "Map" (really just pairs) from RDD ID to the range which its partitions occupy within the UnionRDD above.
          val partitionRangesByRDD: Seq[(Int, Range)] =
            genericRDDs
              .scanLeft((null: RDD[Any], 0)) {
                case ((lastRDD, offset), curRDD) =>
                  val lastRDDPartitions = Option(lastRDD).map(_.getNumPartitions).getOrElse(0)
                  (curRDD: RDD[Any]) -> (offset + lastRDDPartitions)
              }
              .drop(1)
              .map {
                case (rdd, offset) =>
                  rdd.id -> (offset until offset + rdd.getNumPartitions)
              }

          // Compute each partition's size in the UnionRDD.
          val partitionSizes =
            union
              .mapPartitionsWithIndex(
                (index, iterator) => Iterator(index -> iterator.size),
                preservesPartitioning = true
              )
              .collectAsMap

          // For each RDD, infer its size from the partition-sizes computed above.
          for {
            (rddId, partitionsRange) <- partitionRangesByRDD
            rddSize = partitionsRange.map(partitionSizes(_)).sum
          } {
            cache(rddId) = rddSize
          }
      }

  /** Get current cache state; exposed for testing. */
  private[rdd] def getCache: Map[Int, Long] = cache.toMap
}

object CachedCountRegistry {

  private val instances = mutable.HashMap[SparkContext, CachedCountRegistry]()

  private[rdd] def apply(sc: SparkContext): CachedCountRegistry =
    instances.getOrElseUpdate(sc, new CachedCountRegistry)

  // == Implicit API ==
  implicit class SingleRDDCount(rdd: RDD[_]) {
    def size: Long = apply(rdd.sparkContext).cachedCount(rdd)
  }

  implicit class MultiRDDCount(rdds: Seq[RDD[_]]) {
    private lazy val scOpt = rdds.headOption.map(_.sparkContext)

    def total: Long =
      scOpt match {
        case Some(sc) => apply(sc).multiCachedCount(rdds).sum
        case None => 0
      }

    def sizes: Seq[Long] =
      scOpt match {
        case Some(sc) => apply(sc).multiCachedCount(rdds)
        case None => Nil
      }
  }

  class HasMultiRDDCount(rdds: RDD[_]*) {
    protected val multiRDDCount = new MultiRDDCount(rdds)
    def total: Long = multiRDDCount.total
  }

  implicit class Tuple2RDDCount(t: (RDD[_], RDD[_])) extends HasMultiRDDCount(t._1, t._2) {
    def sizes: (Long, Long) = {
      val s = multiRDDCount.sizes
      (s(0), s(1))
    }
  }

  implicit class Tuple3RDDCount(t: (RDD[_], RDD[_], RDD[_])) extends HasMultiRDDCount(t._1, t._2, t._3) {
    def sizes: (Long, Long, Long) = {
      val s = multiRDDCount.sizes
      (s(0), s(1), s(2))
    }
  }
}
