package org.apache.spark.sort

import org.apache.spark.util.{CollectionsUtils => SparkCollectionsUtils}

import scala.reflect.ClassTag

object CollectionsUtils {
  def makeBinarySearch[K : Ordering : ClassTag] : (Array[K], K) => Int =
    SparkCollectionsUtils.makeBinarySearch[K]
}
