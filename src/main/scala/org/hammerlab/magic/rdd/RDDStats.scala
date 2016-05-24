package org.hammerlab.magic.rdd

import org.apache.spark.rdd.{RDD, UnionRDD}
import org.hammerlab.magic.util.Stats

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

private case class PartitionStats[T: ClassTag](boundsOpt: Option[(T, T)], count: Long, isSorted: Boolean)

case class RDDStats[T: ClassTag] private(partitionBounds: ArrayBuffer[Option[(T, T)]],
                                         partitionCounts: ArrayBuffer[Long],
                                         isSorted: Boolean) extends Serializable {
  lazy val countStats = Stats(partitionCounts)
  lazy val nonEmptyCountStats = Stats(partitionCounts.filter(_ > 0))
}

object RDDStats {
  private val rddMap = mutable.Map[Int, RDDStats[_]]()
  implicit def rddToPartitionBoundsRDD[T: ClassTag](
    rdd: RDD[T]
  )(
    implicit ordering: PartialOrdering[T]
  ): RDDStats[T] = {
    rddMap.getOrElseUpdate(
      rdd.id,
      RDDStats[T](rdd)
    ).asInstanceOf[RDDStats[T]]
  }

  def apply[T: ClassTag](partitionStats: Iterable[PartitionStats[T]])(implicit ordering: PartialOrdering[T]): RDDStats[T] = {
    val bounds = ArrayBuffer[Option[(T, T)]]()
    val counts = ArrayBuffer[Long]()

    var prevOpt: Option[T] = None
    var rddIsSorted = true
    for {
      PartitionStats(boundsOpt, count, partitionIsSorted) <- partitionStats
    } {
      val firstOpt = boundsOpt.map(_._1)
      rddIsSorted =
        rddIsSorted &&
          partitionIsSorted &&
          (
            prevOpt.isEmpty ||
              firstOpt.isEmpty ||
              ordering.lteq(prevOpt.get, firstOpt.get)
            )

      bounds += boundsOpt
      counts += count
      prevOpt = boundsOpt.map(_._2)
    }

    RDDStats(bounds, counts, rddIsSorted)
  }

  def apply[T: ClassTag](rdd: RDD[T])(implicit ordering: PartialOrdering[T]): RDDStats[T] = {
    val partitionStats: Array[PartitionStats[T]] =
      rdd.mapPartitions(iter => {
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
          Iterator(PartitionStats[T](Some(first, last), count, partitionIsSorted))
        }
      }).setName(s"partition stats: ${rdd.name}").collect()

    // As an optimization, any time we compute stats for a UnionRDD, cache stats for its dependency-RDDs too.
    rdd match {
      case unionRDD: UnionRDD[T] =>
        var partitionRangeStart = 0
        for {
          dependencyRDD <- unionRDD.rdds
          partitionRangeEnd = partitionRangeStart + dependencyRDD.getNumPartitions
        } {
          rddMap.getOrElseUpdate(
            dependencyRDD.id,
            RDDStats(partitionStats.view.slice(partitionRangeStart, partitionRangeEnd))
          )
          partitionRangeStart = partitionRangeEnd
        }
      case _ =>
    }

    RDDStats(partitionStats)
  }
}
