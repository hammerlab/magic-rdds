package org.hammerlab.magic.rdd.scan

import cats.kernel.Monoid
import org.apache.spark.rdd.RDD
import org.hammerlab.magic.rdd.scan.ScanRightRDD._

class ScanRightRDDReverseTest
  extends ScanRightRDDTest {
  override def useRDDReversal = true
}
