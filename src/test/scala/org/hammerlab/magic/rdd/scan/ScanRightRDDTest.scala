package org.hammerlab.magic.rdd.scan

import org.hammerlab.magic.rdd.scan.ScanRightRDD._

import scala.reflect.ClassTag

class ScanRightRDDTest extends ScanRightRDDTestI {

  def check[T: ClassTag](identity: T,
                         input: Iterable[T],
                         op: (T, T) ⇒ T,
                         expectedOpt: Option[Seq[T]] = None): Unit = {

    val rdd = sc.parallelize(input.toSeq)

    val actualArr =
      rdd
        .scanRight(identity)(op)
        .collect()

    val expectedArr =
      expectedOpt.getOrElse(
        input
          .scanRight(identity)(op)
          .dropRight(1)
          .toArray
      )

    actualArr should ===(expectedArr)
  }
}
