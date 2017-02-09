package org.apache.spark.batch

import org.apache.spark
import org.apache.spark.{ Dependency, HashPartitioner, OneToOneDependency, ShuffleDependency, SparkEnv, TaskContext }
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * [[MapRDD]] is a map-side RDD to serialize partition data, and wait for all parent RDDs to
 * finish. Note that type 'T' must be serializable.
 *
 * @param parent original RDD to batch, not transient because it is used in compute method
 * @param previous previous batch that this RDD needs to wait before starting computation
 * @param batch set/batch of original RDD partitions that this RDD needs to evaluate
 */
class MapRDD[T: ClassTag](
    var parent: RDD[T],
    @transient var previous: Option[MapRDD[T]],
    private val batch: Array[spark.Partition])
  extends RDD[(Int, T)](parent.sparkContext, Nil) {

  // Hash partitioner to spill data, this will create shuffle output per original partition
  // since partitioner uses mod(key, len) function
  val part = new HashPartitioner(parent.partitions.length)

  // We need to remap original partitions, since Spark checks for valid partition indices, e.g.
  // should start from 0 and cover all splits.
  override def getPartitions: Array[spark.Partition] = {
    batch.zipWithIndex.map { case (x, index) =>
      new Partition(this.id, index, x)
    }
  }

  /**
   * Get entire graph of depedencies for this batch RDD, for example
   * original <- RDD1 <- RDD2 <- RDD3 will result in Seq(RDD1, RDD2) for RDD3 and empty sequence
   * for RDD1.
   */
  def getBatchDependencies: Seq[MapRDD[T]] = previous match {
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
      new ShuffleDependency[Int, T, T](
        batchRdd,
        part,
        SparkEnv.get.serializer
      ) :: Nil
    case None =>
      new OneToOneDependency(parent) :: Nil
  }

  // Compute method maps each value as (splitIndex, originalValue), where splitIndex is index of
  // parent/original RDD partition, and originalValue is a value from original RDD for that
  // partition. This will allow to reduce output correctly into the same number of partitions
  override def compute(split: spark.Partition, context: TaskContext): Iterator[(Int, T)] = {
    val partition = split.asInstanceOf[Partition]
    val splitIndex = partition.parent.index
    val iter = parent.iterator(partition.parent, context)
    iter.map { x => (splitIndex, x) }
  }

  override def clearDependencies(): Unit = {
    parent = null
    previous = null
  }
}
