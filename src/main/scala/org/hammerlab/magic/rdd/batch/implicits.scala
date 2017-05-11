package org.hammerlab.magic.rdd.batch

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.batch._

package object implicits {
  implicit class BatchedRDDFunctions[T : ClassTag](rdd: RDD[T]) {
    def batch(numPartitionsPerBatch: Int): RDD[T] = {
      require(numPartitionsPerBatch > 0,
        s"Positive number of partitions per batch is required, found $numPartitionsPerBatch")
      // if requested num partitions is greater than or equal to total number of RDD partitions
      // we just return RDD itself, since it would result in single batch; empty RDD will fall into
      // this condition as well
      if (rdd.partitions.length <= numPartitionsPerBatch) {
        rdd
      } else {
        // total batches generated, do not use sc.defaultParallelism
        val batches = rdd.partitions.sliding(numPartitionsPerBatch, numPartitionsPerBatch)
        // build RDD operations graph, looks like this:
        // reduce → map → map → map → map → mapPartitions → RDD
        var mapRdd: Option[MapRDD[T]] = None
        for (batch ← batches) {
          mapRdd = Some(new MapRDD[T](rdd, mapRdd, batch))
        }
        mapRdd match {
          case Some(mapPart) ⇒
            new ReduceRDD(mapPart)
          case None ⇒
            throw new IllegalStateException(
              "No batches generated for map-side RDD using " +
              s"$numPartitionsPerBatch partitions per batch")
        }
      }
    }
  }
}
