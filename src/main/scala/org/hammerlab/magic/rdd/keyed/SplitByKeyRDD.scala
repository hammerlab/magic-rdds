package org.hammerlab.magic.rdd.keyed

import org.apache.spark.rdd.{PartitionPruningRDD, RDD}
import org.hammerlab.spark.util.KeyPartitioner
import org.hammerlab.magic.rdd.partitions.SlicePartitionsRDD
import org.hammerlab.spark.PartitionIndex

import scala.reflect.ClassTag

/**
 * Add [[splitByKey]] method to any [[RDD]] of pairs: returns a [[Map]] from each key (type [[K]]) to an [[RDD[V]]] with
 * all the values that had that key in the original [[RDD]] (in arbitrary order).
 *
 * The resulting per-key [[RDD]]s have been shuffled to actually be separated from each other on disk, allowing
 * subsequent operations to only have to traverse the values corresponding to a given key (as opposed to a naive
 * approach of calling [[RDD.filter]] on the entire original [[RDD]] once for each key).

 * @param rdd Paired [[RDD]] to split up by key.
 */
case class SplitByKeyRDD[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]) {

  def splitByKey(): Map[K, RDD[V]] = splitByKey(rdd.countByKey().toMap)

  def splitByKey(keyCounts: Map[K, Long]): Map[K, RDD[V]] = {
    val count = keyCounts.values.sum

    val sc = rdd.sparkContext
    val numPartitions = rdd.getNumPartitions

    // Resulting RDDs will be allotted partitions according to the input RDD's average number of elements per partition.
    val elemsPerPartition = math.ceil(count.toDouble / numPartitions).toInt

    /**
     * Two parallel collections: the list of keys, and a list of numbers of partitions to be allotted for each key's
     * [[RDD]].
     */
    val (keys, partitionsPerKey) =
      (for {
        (k, num) ← keyCounts
      } yield
        k → math.ceil(num.toDouble / elemsPerPartition).toInt
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

    val partitionedRDD =
      (for {
        (k, v) ← rdd
        (start, end) = partitionRangesByKeyBroadcast.value(k)
        numPartitions = end - start
        partitionIdx = start + (math.abs((k, v).hashCode()) % numPartitions)
      } yield
        partitionIdx → v
      ).partitionBy(KeyPartitioner(newNumPartitions))

    val partitionedValuesRDD = partitionedRDD.values

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
