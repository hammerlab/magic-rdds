package org.hammerlab.magic.rdd.zip

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Wrapper for [[RDD.zipPartitions]] that fails early if the RDDs' partition-counts don't match
 */
trait ZipPartitions {
  implicit class ZipPartitionsOps[T](rdd: RDD[T]) {
    def zippartitions[U: ClassTag, V: ClassTag](other: RDD[U])(f: (Iterator[T], Iterator[U]) ⇒ Iterator[V]): RDD[V] =
      if (rdd.getNumPartitions == other.getNumPartitions)
        rdd.zipPartitions(other)(f)
      else
        throw ZipRDDDifferingPartitionsException(Seq(rdd, other))

    def zippartitions[U: ClassTag, V: ClassTag, W: ClassTag](other1: RDD[U],
                                                             other2: RDD[V])(
        f: (Iterator[T], Iterator[U], Iterator[V]) ⇒ Iterator[W]
    ): RDD[W] =
      if (rdd.getNumPartitions == other1.getNumPartitions && other1.getNumPartitions == other2.getNumPartitions)
        rdd.zipPartitions(other1, other2)(f)
      else
        throw ZipRDDDifferingPartitionsException(
          Seq(
            rdd,
            other1,
            other2
          )
        )
  }
}
