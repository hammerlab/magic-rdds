package org.hammerlab.magic.rdd.grid

import cats.Monoid
import hammerlab.monoid._
import org.apache.spark.rdd.RDD
import org.hammerlab.kryo._
import org.hammerlab.magic.rdd.grid.PrefixSum.GridRDD

import scala.reflect.ClassTag

trait PrefixSum {
  implicit class PrefixSumOps[V: ClassTag : Monoid](rdd: GridRDD[V]) {
    def prefixSum2D(partitionDimensionsOpt: Option[(Int, Int)] = None): Result[V] = Result(rdd, partitionDimensionsOpt)
  }
}

object PrefixSum
  extends Registrar {

  type Row = Int
  type Col = Int

  type GridRDD[V] = RDD[((Row, Col), V)]

  register(
    cls[BottomLeftElem[_]],
    cls[BottomRow[_]],
    cls[LeftCol[_]]
  )
}
