package org.hammerlab.magic.rdd.keyed

import org.apache.spark.rdd.{PartitionPruningRDD, RDD}
import org.hammerlab.magic.rdd.KeyPartitioner

import scala.reflect.ClassTag

/**
 * Add [[splitByKey]] method to any paired [[RDD]]: returns a [[Map]] from each key (type [[K]]) to an [[RDD[V]]] with
 * all the values that had that key in the original [[RDD]] (in arbitrary order).
 *
 * The resulting per-key [[RDD]]s have been shuffled to actually be separated from each other on disk, allowing
 * subsequent operations to only have to traverse the values corresponding to a given key (as opposed to naive approach
 * that called [[RDD.filter]] on the entire original [[RDD]] once for each key).

 * @param rdd Paired [[RDD]] to split up by key.
 */
class SplitByKeyRDD[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]) {

  def splitByKey(): Map[K, RDD[V]] = splitByKey(rdd.countByKey().toMap)

  def splitByKey(keyCounts: Map[K, Long]): Map[K, RDD[V]] = {
    val count = keyCounts.values.sum

    val numPartitions = rdd.getNumPartitions

    val elemsPerPartition = math.ceil(count.toDouble / numPartitions).toInt

    val (keys, partitionsPerKey) =
      (for {
        (k, num) <- keyCounts
      } yield
        k -> math.ceil(num.toDouble / elemsPerPartition).toInt
      ).toVector.unzip

    val partitionRangeBoundaries = partitionsPerKey.scanLeft(0)(_ + _)

    val newNumPartitions = partitionRangeBoundaries.last

    val partitionRangesByKey =
      (for {
        (k, bounds) <- keys.iterator.zip(partitionRangeBoundaries.sliding(2))
        start = bounds(0)
        end = bounds(1)
      } yield
        k -> (start, end)
      ).toMap

    val sc = rdd.sparkContext

    val partitionRangesByKeyBroadcast = sc.broadcast(partitionRangesByKey)

    val partitionedRDD =
      (for {
        (k, v) <- rdd
        (start, end) = partitionRangesByKeyBroadcast.value(k)
        numPartitions = end - start
        partitionIdx = start + (math.abs((k, v).hashCode()) % numPartitions)
      } yield
        partitionIdx -> v
      ).partitionBy(KeyPartitioner(newNumPartitions))

    val partitionedValuesRDD = partitionedRDD.values

    for {
      (k, (start, end)) <- partitionRangesByKey
    } yield
      k -> (new SlicePartitionsRDD(partitionedValuesRDD, start, end): RDD[V])
  }
}

object SplitByKeyRDD {
  implicit def toSplitByKeyRDD[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]): SplitByKeyRDD[K, V] =
    new SplitByKeyRDD(rdd)
}

class SlicePartitionsRDD[T: ClassTag](prev: RDD[T], start: Int, end: Int)
  extends PartitionPruningRDD[T](prev, idx => start <= idx && idx < end)

