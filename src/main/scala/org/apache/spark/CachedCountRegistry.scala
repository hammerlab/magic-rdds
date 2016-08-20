package org.apache.spark

import org.apache.spark.rdd.{RDD, UnionPartition, UnionRDD}

import scala.collection.mutable

/**
 * [[CachedCountRegistry]] provides functionality to run count and cache result for single RDD, and
 * run single job for several RDDs with intermediate cache of those RDDs. If one of the RDDs is
 * already counted, registry will pull cache instead of recomputing it again.
 *
 * Note: use 'size()' when applying to multiple RDDs, otherwise it will return list length.
 * Usage:
 * {{{
 * import org.apache.spark.CachedCountRegistry
 * import org.apache.spark.CachedCountRegistry._
 * val rdd1 = sc.parallelize(0 until 4)
 * val rdd2 = sc.parallelize("a" :: "b" :: Nil)
 * rdd1.size()
 * (rdd1 :: rdd2 :: Nil).size()
 * }}}
 */
object CachedCountRegistry {
  private val cache = mutable.HashMap[Int, Long]()

  /** Count RDD and cache result for RDD id */
  def cachedCount(rdd: RDD[_]): Long = multiCachedCount(List(rdd))

  /**
   * Count multi RDDs as single job and cache count separately, return total count. This method
   * will construct UnionRDD from only non-cached RDDs
   */
  def multiCachedCount(rdds: Seq[RDD[_]]): Long = {

    val (cachedRDDs, nonCachedRDDs) = rdds.partition(rdd => cache.contains(rdd.id))

    // pull whatever cached counts we have
    val cachedCount = cachedRDDs.map(rdd => cache(rdd.id)).sum

    val unionRDDChildrenMap = mutable.HashMap[Int, Seq[Int]]()
    val expandedRDDs =
      nonCachedRDDs.flatMap {
        case unionRDD: UnionRDD[_] =>
          val children = unionRDD.rdds: Seq[RDD[_]]
          unionRDDChildrenMap(unionRDD.id) = children.map(_.id)
          children
        case rdd: RDD[_] => List(rdd)
      }

    cachedCount +
      (if (expandedRDDs.length > nonCachedRDDs.length) {
        val count = multiCachedCount(expandedRDDs)
        for {
          (unionRDDId, childrenIDs) <- unionRDDChildrenMap
        } {
          cache(unionRDDId) = childrenIDs.map(cache(_)).sum
        }
        count
      } else
        if (nonCachedRDDs.nonEmpty) {
          val (union, partitionsMap) = makeUnionRDD(nonCachedRDDs)
          internalMultiCachedCount(union, partitionsMap)
        } else
          0L
      )
  }

  /** Get current cache state */
  def getCache(): mutable.HashMap[Int, Long] = cache.clone()

  def resetCache(): Unit = cache.clear()

  // == Implicit API ==
  implicit class SingleRDDCount(rdd: RDD[_]) {
    def size(): Long = cachedCount(rdd)
  }

  implicit class MultiRDDCount(rdds: List[RDD[_]]) {
    def size(): Long = multiCachedCount(rdds)
  }

  /** Internal multi count cache for UnionRDD */
  private def internalMultiCachedCount(rdd: UnionRDD[_], partitionsMap: Map[Int, Array[Int]]): Long = {
    // each partition has unique index, so we do not expect collisions there
    val counts =
      rdd.mapPartitionsWithIndex(
        (index, iterator) => Iterator(index -> iterator.size),
        preservesPartitioning = true
      ).collectAsMap

    var totalCount = 0L
    partitionsMap.foreach { case (rddId, partitions) =>
      var rddCount = 0L
      partitions.foreach { index =>
        rddCount +=
          counts.getOrElse(
            index,
            sys.error(s"Partition index $index for parent $rddId is not found in UnionRDD")
          )
      }

      // cache count and update total
      totalCount += cache.getOrElseUpdate(rddId, rddCount)
    }

    totalCount
  }

  /** Make generic UnionRDD of type Any, return union RDD and partitions map of RDD id to index in union */
  private def makeUnionRDD(rdds: Seq[RDD[_]]): (UnionRDD[_], Map[Int, Array[Int]]) = {
    require(rdds.nonEmpty, "Non-empty list of RDDs required for union")
    val sc = rdds.head.sparkContext
    val genericRDDs = rdds.map(_.asInstanceOf[RDD[Any]])
    val union = sc.union(genericRDDs).asInstanceOf[UnionRDD[_]]
    // reconstruct map of RDD id and corresponding partitions
    // map to keep order index -> RDD id connection, this assumes that RDDs are indexed exactly
    // like in UnionRDD (which is always true for the same sequence), otherwise it will assign
    // wrong indices
    val indices = genericRDDs.map(_.id).zipWithIndex.map(_.swap).toMap
    val partitions = union.partitions.map(_.asInstanceOf[UnionPartition[_]])
    // return map of RDD index -> array of partition indices in UnionRDD
    val partitionsMap = partitions.map { part =>
      val rddId = indices.getOrElse(part.parentRddIndex,
        sys.error(s"Cannot find parent RDD index ${part.parentRddIndex} in map $indices"))
      (rddId, part.index)
    }.groupBy(_._1).mapValues(_.map(_._2))

    (union, partitionsMap)
  }
}
