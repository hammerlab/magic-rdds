package org.apache.spark.context

import org.apache.spark.SparkContext

object Util {
  implicit class Cleanable(sc: SparkContext) {
    def clean[F <: AnyRef](f: F, checkSerializable: Boolean = true): F =
      sc.clean(f, checkSerializable)
  }
}
