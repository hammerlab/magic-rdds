package org.hammerlab.magic.rdd.partitions

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.SparkContext
import org.apache.spark.rdd.{RDD, UnionRDD}
import org.hammerlab.stats.Stats

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

sealed abstract class RDDStats[T: ClassTag] {
  /** For each partition, holds the size of the partition. */
  def partitionSizes: Seq[Long]

  lazy val countStats = Stats(partitionSizes)
  lazy val nonEmptyCountStats = Stats(partitionSizes.filter(_ > 0))
}

/**
 * @param partitionBounds For each partition, an option that is empty iff the partition is empty, and contains the first
 *                        and last elements otherwise.
 */
case class SortedRDDStats[T: ClassTag] private[partitions](partitionBounds: Seq[Option[(T, T)]],
                                                           partitionSizes: Seq[Long])
  extends RDDStats[T]
    with Serializable

case class UnsortedRDDStats[T: ClassTag] private[partitions](partitionSizes: Seq[Long])
  extends RDDStats[T]
    with Serializable

private case class PartitionStats[T: ClassTag](boundsOpt: Option[(T, T)], count: Long, isSorted: Boolean)

object RDDStats {

  def register(kryo: Kryo): Unit = {
    kryo.register(classOf[Array[PartitionStats[_]]])
    kryo.register(classOf[PartitionStats[_]])
  }

  private val rddMap = mutable.Map[(SparkContext, Int), RDDStats[_]]()
  implicit def rddToPartitionBoundsRDD[T: ClassTag](
    rdd: RDD[T]
  )(
    implicit ordering: PartialOrdering[T]
  ): RDDStats[T] =
    rddMap.getOrElseUpdate(
      (rdd.sparkContext, rdd.id),
      RDDStats[T](rdd)
    ).asInstanceOf[RDDStats[T]]

  private def apply[T: ClassTag](
    partitionStats: Iterable[PartitionStats[T]]
  )(
    implicit ordering: PartialOrdering[T]
  ): RDDStats[T] = {

    val bounds = ArrayBuffer[Option[(T, T)]]()
    val counts = ArrayBuffer[Long]()

    var prevUpperBoundOpt: Option[T] = None
    var rddIsSorted = true

    for {
      PartitionStats(boundsOpt, count, partitionIsSorted) <- partitionStats
    } {
      rddIsSorted =
        rddIsSorted &&
          partitionIsSorted &&
          (
            // If the current partition has a lower bound which is less than the previous partition's upper bound,
            // flag RDD as not-sorted.
            (prevUpperBoundOpt, boundsOpt) match {
              case (Some(prevUpperBound), Some((curLowerBound, _))) => !ordering.gt(prevUpperBound, curLowerBound)
              case _ => true
            }
          )

      bounds += boundsOpt
      counts += count
      prevUpperBoundOpt = boundsOpt.map(_._2)
    }

    if (rddIsSorted)
      SortedRDDStats(bounds, counts)
    else
      UnsortedRDDStats(counts)
  }

  def apply[T: ClassTag](rdd: RDD[T])(implicit ordering: PartialOrdering[T]): RDDStats[T] = {
    val partitionStats: Array[PartitionStats[T]] =
      rdd.mapPartitions(
        iter => {
          if (iter.isEmpty) {
            Iterator(PartitionStats[T](None: Option[(T, T)], 0, isSorted = true))
          } else {
            val first = iter.next()

            var partitionIsSorted = true
            var last = first
            var count = 1
            while (iter.hasNext) {
              val prev = last
              last = iter.next()
              count += 1
              if (partitionIsSorted && ordering.gt(prev, last)) {
                partitionIsSorted = false
              }
            }

            Iterator(
              PartitionStats[T](
                Some(first, last),
                count,
                partitionIsSorted
              )
            )
          }
        }
      )
      .setName(s"partition-stats: ${rdd.name}")
      .collect()

    // As an optimization, any time we compute stats for a UnionRDD, cache stats for its dependency-RDDs too.
    rdd match {
      case unionRDD: UnionRDD[T] =>
        var partitionRangeStart = 0
        for {
          dependencyRDD <- unionRDD.rdds
          partitionRangeEnd = partitionRangeStart + dependencyRDD.getNumPartitions
        } {
          rddMap.getOrElseUpdate(
            (dependencyRDD.sparkContext, dependencyRDD.id),
            RDDStats(partitionStats.view.slice(partitionRangeStart, partitionRangeEnd))
          )
          partitionRangeStart = partitionRangeEnd
        }
      case _ =>
    }

    RDDStats(partitionStats)
  }
}
