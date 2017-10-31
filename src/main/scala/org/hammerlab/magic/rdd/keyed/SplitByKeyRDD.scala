package org.hammerlab.magic.rdd.keyed

import magic_rdds.partitions._
import org.apache.spark.rdd.RDD
import org.hammerlab.iterator.CountIteratorByKey._
import org.hammerlab.iterator.sliding.Sliding2Iterator._
import org.hammerlab.magic.rdd.partitions.SlicePartitionsRDD
import org.hammerlab.spark.PartitionIndex

import scala.collection.mutable
import scala.math.ceil
import scala.reflect.ClassTag

/**
 * Add [[splitByKey]] method to any [[RDD]] of pairs: returns a [[Map]] from each key ([[K]]) to an [[RDD[V]]] with
 * all the values that had that key in the original [[RDD]] (with relative order preserved for each key).
 *
 * One shuffle stage on all keys and their values yields an [[RDD]] whose partitions are arranged in disjoint,
 * contiguous regions corresponding to all the values for each key; this is much more efficient than a naive approach to
 * separating [[RDD]]s by key: performing an [[RDD.filter]] for each key in the [[RDD]];.
 *
 * However, it's worth noting that breaking up an [[RDD]] into a collection of [[RDD]]s in this way is fairly
 * unidiomatic, and if one finds themselves wanting this it's worth pausing and considering taking different actions
 * upstream.
 *
 * @param rdd Paired [[RDD]] to split up by key.
 */
case class SplitByKeyRDD[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]) {

  def splitByKey: Map[K, RDD[V]] = {

    val cumulativeKeyCounts =
      rdd
        .collapsePartitions(_.countByKey)
        .scanLeft(Map.empty[K, Long])(
          (soFar, partitionCounts) ⇒
            soFar ++
              (for {
                (key, count) ← partitionCounts
              } yield
                key → (count + soFar.getOrElse(key, 0L))
              )
        )

    val totalKeyCounts = cumulativeKeyCounts.last

    val count = totalKeyCounts.values.sum

    val sc = rdd.sparkContext
    val numPartitions = rdd.getNumPartitions

    val cumulativeKeyCountsRDD =
      sc
        .parallelize(
          cumulativeKeyCounts.dropRight(1),
          numPartitions
        )

    // Resulting RDDs will be allotted partitions according to the input RDD's average number of elements per partition.
    val elemsPerPartition = math.ceil(count.toDouble / numPartitions).toInt

    /**
     * Two parallel collections: the list of keys, and a list of numbers of partitions to be allotted for each key's
     * [[RDD]].
     */
    val (keys, partitionsPerKey) =
      (for {
        (k, num) ← totalKeyCounts
      } yield
        k → ceil(num.toDouble / elemsPerPartition).toInt
      )
      .toVector
      .unzip

    val partitionRangeBoundaries = partitionsPerKey.scanLeft(0)(_ + _)

    val newNumPartitions = partitionRangeBoundaries.last

    val partitionRangesByKey: Map[K, (PartitionIndex, PartitionIndex)] =
      (for {
        (k, (start, end)) ←
          keys
            .iterator
            .zip(partitionRangeBoundaries.sliding2)
      } yield
        k → (start, end)
      )
      .toMap

    val partitionRangesByKeyBroadcast = sc.broadcast(partitionRangesByKey)

    val indexedRDD =
      rdd
        .zipPartitions(cumulativeKeyCountsRDD) {
          (it, prefixSumIter) ⇒
            val partitionRangesByKey = partitionRangesByKeyBroadcast.value
            val prefixSum = mutable.Map[K, Long](prefixSumIter.next.toSeq: _*)
            for {
              (k, v) ← it
              idx = prefixSum.getOrElse(k, 0L)
              (start, _) = partitionRangesByKey(k)
              newPartition = start + (idx / elemsPerPartition).toInt
            } yield {
              prefixSum(k) = idx + 1
              newPartition → idx → v
            }
        }
        .partitionByKey(newNumPartitions)

    for {
      (k, (start, end)) ← partitionRangesByKey
    } yield
      k →
        (
          SlicePartitionsRDD(
            indexedRDD,
            start,
            end
          ): RDD[V]
        )
  }
}

object SplitByKeyRDD {
  implicit def makeSplitByKeyRDD[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]): SplitByKeyRDD[K, V] = SplitByKeyRDD(rdd)
}
