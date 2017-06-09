package org.hammerlab.parallel.spark

import org.hammerlab.parallel
import org.hammerlab.spark.test.suite.SparkSuite

class ParallelizerTest
  extends SparkSuite
    with parallel.ParallelizerTest {

  implicit lazy val config = Config()
  import parallel.makeParallelizer

  override def make(arr: Array[Int]): Array[String] =
    arr.pmap(_.toString)
}

