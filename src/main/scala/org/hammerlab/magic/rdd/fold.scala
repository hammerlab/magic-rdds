package org.hammerlab.magic.rdd

import cats.Monoid
import org.apache.spark.rdd.RDD

trait fold {
  implicit class FoldOps[T](rdd: RDD[T]) extends Serializable {
    def fold(implicit m: Monoid[T]): T = rdd.fold(m.empty)(m.combine)
  }
}
