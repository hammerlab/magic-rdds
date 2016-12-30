package org.hammerlab.magic.rdd.cache

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.reflect.ClassTag

abstract class RDDCache[T: ClassTag, U] {

  def apply(rdd: RDD[T]): U = cache.getOrElseUpdate(rdd, compute(rdd))

  protected implicit def unwrapRDD(rdd: RDD[_]): (SparkContext, Int) = (rdd.sparkContext, rdd.id)

  private val cache = mutable.HashMap[(SparkContext, Int), U]()

  protected def contains(rdd: RDD[T]): Boolean = cache.contains(rdd)

  protected def compute(rdd: RDD[T]): U

  protected final def update(pair: (RDD[T], U)): Unit = cache.update(pair._1, pair._2)
  protected final def update(rdd: RDD[T], value: U): Unit = cache.update(rdd, value)

  /** Get current cache state; exposed for testing. */
  def getCache(implicit sc: SparkContext): Map[Int, U] =
    for {
      ((context, id), value) ← cache.toMap
      if sc == context
    } yield
      id → value
}

abstract class MultiRDDCache[T: ClassTag, U] extends RDDCache[T, U] {

  def apply(rdds: Seq[RDD[T]]): Seq[U] = {
    val uncachedRDDs = rdds.filterNot(contains)

    uncachedRDDs
      .zip(compute(uncachedRDDs))
      .foreach(update)

    rdds.map(apply)
  }

  override protected def compute(rdd: RDD[T]): U = compute(Seq(rdd)).head

  protected def compute(rdds: Seq[RDD[T]]): Seq[U]
}
