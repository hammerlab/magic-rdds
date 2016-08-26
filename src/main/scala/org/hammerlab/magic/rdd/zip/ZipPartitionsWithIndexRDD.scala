package org.hammerlab.magic.rdd.zip

import org.apache.spark.rdd.RDD
import org.apache.spark.context.Util.Cleanable

import scala.reflect.ClassTag


class ZipPartitionsWithIndexRDD[T: ClassTag](@transient val rdd: RDD[T]) extends Serializable {

  @transient private val sc = rdd.sparkContext

  def zipPartitionsWithIndex[U: ClassTag, V: ClassTag](rdd2: RDD[U], preservesPartitioning: Boolean = false)
                                                      (f: (Int, Iterator[T], Iterator[U]) => Iterator[V]): RDD[V] = {
    new ZippedPartitionsWithIndexRDD2(sc, sc.clean(f), rdd, rdd2, preservesPartitioning)
  }

  def zipPartitionsWithIndex[U: ClassTag, V: ClassTag, W: ClassTag](
    rdd2: RDD[U], rdd3: RDD[V], preservesPartitioning: Boolean = false
  )(f: (Int, Iterator[T], Iterator[U], Iterator[V]) => Iterator[W]): RDD[W] = {
    new ZippedPartitionsWithIndexRDD3(sc, sc.clean(f), rdd, rdd2, rdd3, preservesPartitioning)
  }
}

object ZipPartitionsWithIndexRDD {
  implicit def rddToZipPartitionsWithIndexRDD[T: ClassTag](rdd: RDD[T]): ZipPartitionsWithIndexRDD[T] =
    new ZipPartitionsWithIndexRDD(rdd)
}
