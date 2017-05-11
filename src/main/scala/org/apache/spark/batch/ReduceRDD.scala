package org.apache.spark.batch

import org.apache.spark
import org.apache.spark.{ Dependency, MapOutputTrackerMaster, ShuffleDependency, SparkEnv, TaskContext }
import org.apache.spark.rdd.RDD

import scala.collection.mutable.{ HashMap ⇒ MutableMap }
import scala.reflect.ClassTag

/**
 * [[ReduceRDD]] reduces each output from [[MapRDD]] and returns RDD that has original
 * number of partitions and similar data distribution, meaning it is safe to rely on the same order
 * of data in each partition.
 *
 * @param rdd RDD of last map-side stage in chain of batches
 */
class ReduceRDD[T: ClassTag](
    @transient var rdd: MapRDD[T])
  extends RDD[T](rdd) {

  // Index to map original partitions to shuffle dependency that points to shuffle output for that
  // partition.
  private val shuffleSplitIndex: Map[Int, Dependency[_]] = buildShuffleDependencies()

  override def getPartitions: Array[spark.Partition] = rdd.parent.partitions

  override def getDependencies: Seq[Dependency[_]] = {
    // same issue with serializer in Spark 1.x, when migrating to Spark 2.x remove Some() wrapper
    new ShuffleDependency[Int, T, T](rdd, rdd.part, SparkEnv.get.serializer,
      None, None, false) :: Nil
  }

  private def buildShuffleDependencies(): Map[Int, Dependency[_]] = {
    // build index of partition index and batch rdd
    // all RDDs that this reduce step depends on
    val rddDependencies = this.rdd.getBatchDependencies :+ this.rdd
    // index of original partition index to map-side RDD
    val batchPartitionIndex = new MutableMap[Int, RDD[_]]()
    rddDependencies.foreach { rdd ⇒
      rdd.getPartitions.foreach {
        case bpart: Partition ⇒
          // batch partition index is not unique, but original partition index is, here we also do
          // some sanity check to make sure that there are no two or more batch partitions that
          // compute the same original partition
          if (batchPartitionIndex.contains(bpart.parent.index)) {
            throw new IllegalStateException(s"Map-side RDD ${bpart.rddId} contains duplicate " +
              s"partition ${bpart.parent} (${bpart.parent.index}) that maps to $bpart. This " +
              "implies that batch map was evaluated more than once for original partition")
          }
          batchPartitionIndex.put(bpart.parent.index, rdd)
        case other ⇒
          sys.error(s"Unexpected partition $other found that is not batch partition")
      }
    }

    // build index for shuffle dependencies for RDD deps
    // we also need to add this reduce-side dependency to build full map
    val shuffleIndex = new MutableMap[RDD[_], Dependency[_]]()
    (rddDependencies :+ this).foreach { rdd ⇒
      rdd.dependencies.foreach {
        case shuffleDep: ShuffleDependency[_, _, _] ⇒
          shuffleIndex.put(shuffleDep.rdd, shuffleDep)
        case otherDep ⇒ // no-op for one-to-one or range dependencies
      }
    }

    // merge two data structures
    val partitionMap: Map[Int, Dependency[_]] = batchPartitionIndex.map { case (partIndex, rdd) ⇒
      // extract shuffle dependency associated with map-side RDD, this is different than looking up
      // dependencies for that RDD, since we are looking for shuffle that has that rdd as dependency
      val shuffleDep = shuffleIndex.getOrElse(rdd, sys.error(s"Failed to find shuffle for " +
        s"rdd $rdd ${rdd.id} when resolving partition index $partIndex"))
      (partIndex, shuffleDep)
    }.toMap

    // check that we covered all original partitions
    if (partitionMap.keys.size != this.getPartitions.length) {
      throw new IllegalStateException(
        s"Partition-dependency map has ${partitionMap.keys.size} partitions, but RDD should " +
        s"have ${this.getPartitions.length} partitions; map = $partitionMap")
    }

    partitionMap
  }

  override protected def getPreferredLocations(partition: spark.Partition): Seq[String] = {
    val locatedDependency = shuffleSplitIndex.getOrElse(partition.index,
      sys.error(s"Failed to locate shuffle dependency for $partition (${partition.index})"))
    val locatedShuffleDependency = locatedDependency.asInstanceOf[ShuffleDependency[_, _, _]]
    val tracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    tracker.getPreferredLocationsForShuffle(locatedShuffleDependency, partition.index)
  }

  // Each dependency is assumed to shuffle dependency
  private def iteratorForDependency(
      dep: Dependency[_], partition: spark.Partition, context: TaskContext): Iterator[_] = {
    SparkEnv.get.shuffleManager.
      getReader(dep.asInstanceOf[ShuffleDependency[_, _, _]].shuffleHandle,
        partition.index, partition.index + 1, context).read()
  }

  // Read shuffle output and remap each value to remove partition index
  override def compute(split: spark.Partition, context: TaskContext): Iterator[T] = {
    val shuffleDep = shuffleSplitIndex.getOrElse(split.index,
      sys.error(s"Failed to locate rdd for partition $split (${split.index})"))
    var iter: Iterator[T] = Iterator.empty
    // join all iterators for dependencies
    for (dependency ← dependencies) {
      iter = iter ++ iteratorForDependency(shuffleDep, split, context).
        asInstanceOf[Iterator[(Int, T)]].map { case (index, value) ⇒ value }
    }
    iter
  }

  override def clearDependencies(): Unit = {
    rdd = null
  }
}
