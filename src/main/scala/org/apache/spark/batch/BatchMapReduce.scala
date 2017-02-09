package org.apache.spark.batch

import scala.collection.mutable.{ArrayBuffer, HashMap => MutableMap}
import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.shuffle._
import org.apache.spark.rdd.RDD

// RDD batching is introduced for reason of overcoming OOMs when scheduling all tasks for RDD having
// limited amount of memory per executor. This is a perfect fit for training model to still leverage
// Spark parallelism, but avoid collecting on a driver; this is a tradeoff of computation time and
// memory usage per executor and driver.
//
// Batching of tasks in Spark works as batches mapped to multiple stages. Batch resolution is left
// outside of RDD. Batching is split into two parts: 1 to N map stages and single reduce stage. Map
// stage modifies values to add partition index that is used as key-type for shuffle, therefore we
// serialize data per partition. Each map stage has depedency on itself, but does not pull data from
// shuffle reader. This allows to block execution of other partitions until ith batch is finished.
// Once all map stages are complete, reduce stage is launched to collect all shuffle output and
// remove partition index from values.

/**
 * [[BatchPartition]] is a mirror for parent partition of original RDD, and keeps track of partition
 * task, so we can reconstruct partitions on reduce stage.
 */
class BatchPartition(
    val rddId: Long,
    val slice: Int,
    val parent: Partition)
  extends Partition with Serializable {

  override def hashCode: Int = (41 * (41 + rddId) + slice).toInt

  override def equals(other: Any): Boolean = other match {
    case that: BatchPartition => this.rddId == that.rddId && this.slice == that.slice
    case _ => false
  }

  override def index: Int = slice

  override def toString: String = {
    s"${getClass.getSimpleName}(rddId=$rddId, index=$index, parent=$parent)"
  }
}

/**
 * [[BatchMapRDD]] is a map-side RDD to serialize partition data, and wait for all parent RDDs to
 * finish. Note that type 'T' must be serializable.
 * @param parent original RDD to batch, not transient because it is used in compute method
 * @param previous previous batch that this RDD needs to wait before starting computation
 * @param batch set/batch of original RDD partitions that this RDD needs to evaluate
 */
class BatchMapRDD[T: ClassTag](
    var parent: RDD[T],
    @transient var previous: Option[BatchMapRDD[T]],
    private val batch: Array[Partition])
  extends RDD[(Int, T)](parent.sparkContext, Nil) {

  // Hash partitioner to spill data, this will create shuffle output per original partition
  // since partitioner uses mod(key, len) function
  val part = new HashPartitioner(parent.partitions.length)

  // We need to remap original partitions, since Spark checks for valid partition indices, e.g.
  // should start from 0 and cover all splits.
  override def getPartitions: Array[Partition] = {
    batch.zipWithIndex.map { case (x, index) =>
      new BatchPartition(this.id, index, x)
    }
  }

  /**
   * Get entire graph of depedencies for this batch RDD, for example
   * original <- RDD1 <- RDD2 <- RDD3 will result in Seq(RDD1, RDD2) for RDD3 and empty sequence
   * for RDD1.
   */
  def getBatchDependencies: Seq[BatchMapRDD[T]] = previous match {
    case Some(batchRdd) => batchRdd.getBatchDependencies ++ Seq(batchRdd)
    case None => Seq.empty
  }

  // Extract depedencies of map-side RDD, if this is the first batch, it maps directly to parent,
  // otherwise everything is shuffle dependency on its 'previous' RDD. It is important to reuse
  // the same partitioner for each shuffle depedency.
  override def getDependencies: Seq[Dependency[_]] = previous match {
    case Some(batchRdd) =>
      // do not enable sort or map-side aggregation
      // in Spark 1.x serializer is passed as Option, but in Spark 2.x it is just passed directly,
      // when migrating to Spark 2.x just remove Some() wrapper
      new ShuffleDependency[Int, T, T](batchRdd, part,
        SparkEnv.get.serializer, None, None, false) :: Nil
    case None =>
      new OneToOneDependency[T](parent) :: Nil
  }

  // Compute method maps each value as (splitIndex, originalValue), where splitIndex is index of
  // parent/original RDD partition, and originalValue is a value from original RDD for that
  // partition. This will allow to reduce output correctly into the same number of partitions
  override def compute(split: Partition, context: TaskContext): Iterator[(Int, T)] = {
    val partition = split.asInstanceOf[BatchPartition]
    val splitIndex = partition.parent.index
    val iter = parent.iterator(partition.parent, context)
    iter.map { x => (splitIndex, x) }
  }

  override def clearDependencies(): Unit = {
    parent = null
    previous = null
  }
}

/**
 * [[BatchReduceRDD]] reduces each output from [[BatchMapRDD]] and returns RDD that has original
 * number of partitions and similar data distribution, meaning it is safe to rely on the same order
 * of data in each partition.
 * @param rdd RDD of last map-side stage in chain of batches
 */
class BatchReduceRDD[T: ClassTag](
    @transient var rdd: BatchMapRDD[T])
  extends RDD[T](rdd) {

  // Index to map original partitions to shuffle dependency that points to shuffle output for that
  // partition.
  private val shuffleSplitIndex: Map[Int, Dependency[_]] = buildShuffleDependencies()

  override def getPartitions: Array[Partition] = rdd.parent.partitions

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
    rddDependencies.foreach { rdd =>
      rdd.getPartitions.foreach {
        case bpart: BatchPartition =>
          // batch partition index is not unique, but original partition index is, here we also do
          // some sanity check to make sure that there are no two or more batch partitions that
          // compute the same original partition
          if (batchPartitionIndex.contains(bpart.parent.index)) {
            throw new IllegalStateException(s"Map-side RDD ${bpart.rddId} contains duplicate " +
              s"partition ${bpart.parent} (${bpart.parent.index}) that maps to $bpart. This " +
              "implies that batch map was evaluated more than once for original partition")
          }
          batchPartitionIndex.put(bpart.parent.index, rdd)
        case other =>
          sys.error(s"Unexpected partition $other found that is not batch partition")
      }
    }

    // build index for shuffle dependencies for RDD deps
    // we also need to add this reduce-side dependency to build full map
    val shuffleIndex = new MutableMap[RDD[_], Dependency[_]]()
    (rddDependencies :+ this).foreach { rdd =>
      rdd.dependencies.foreach {
        case shuffleDep: ShuffleDependency[_, _, _] =>
          shuffleIndex.put(shuffleDep.rdd, shuffleDep)
        case otherDep => // no-op for one-to-one or range dependencies
      }
    }

    // merge two data structures
    val partitionMap: Map[Int, Dependency[_]] = batchPartitionIndex.map { case (partIndex, rdd) =>
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

  override protected def getPreferredLocations(partition: Partition): Seq[String] = {
    val locatedDependency = shuffleSplitIndex.getOrElse(partition.index,
      sys.error(s"Failed to locate shuffle dependency for $partition (${partition.index})"))
    val locatedShuffleDependency = locatedDependency.asInstanceOf[ShuffleDependency[_, _, _]]
    val tracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    tracker.getPreferredLocationsForShuffle(locatedShuffleDependency, partition.index)
  }

  // Each dependency is assumed to shuffle dependency
  private def iteratorForDependency(
      dep: Dependency[_], partition: Partition, context: TaskContext): Iterator[_] = {
    SparkEnv.get.shuffleManager.
      getReader(dep.asInstanceOf[ShuffleDependency[_, _, _]].shuffleHandle,
        partition.index, partition.index + 1, context).read()
  }

  // Read shuffle output and remap each value to remove partition index
  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val shuffleDep = shuffleSplitIndex.getOrElse(split.index,
      sys.error(s"Failed to locate rdd for partition $split (${split.index})"))
    var iter: Iterator[T] = Iterator.empty
    // join all iterators for dependencies
    for (dependency <- dependencies) {
      iter = iter ++ iteratorForDependency(shuffleDep, split, context).
        asInstanceOf[Iterator[(Int, T)]].map { case (index, value) => value }
    }
    iter
  }

  override def clearDependencies(): Unit = {
    rdd = null
  }
}
