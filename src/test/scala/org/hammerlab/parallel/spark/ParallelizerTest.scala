package org.hammerlab.parallel.spark

import org.hammerlab.math.ceil
import org.hammerlab.parallel
import org.hammerlab.parallel.spark.SerdeBuffer.{ numCloses, numOpens }
import org.hammerlab.spark.LongAccumulator
import org.hammerlab.spark.test.suite.SparkSuite

class ParallelizerTest
  extends SparkSuite
    with parallel.ParallelizerTest {

  implicit val partitioningStrategy = ElemsPerPartition(2)
  implicit lazy val config = Config()

  import parallel.makeParallelizer

  before {
    numOpens.reset()
    numCloses.reset()
  }

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    sc.register(numOpens)
    sc.register(numCloses)
  }

  override def make(arr: Array[Int]): Array[String] =
    arr.parallel(
      numOpens.add(1),
      _.toString,
      numCloses.add(1)
    )

  override def check(arr: Array[Int]): Unit = {
    super.check(arr)
    numOpens.value should be(ceil(arr.length, 2))
    numCloses.value should be(ceil(arr.length, 2))
  }
}

/**
 * Need to keep these [[org.apache.spark.util.LongAccumulator]]s siloed over here because they get put in closures and
 * sent to Spark tasks, the closure-serializer sweeps up their parent object, and [[ParallelizerTest]] above isn't
 * serializable due to (arguably) a scalatest bug, so they can't be fields of [[ParallelizerTest]].
 */
object SerdeBuffer {
  lazy val numOpens = LongAccumulator()
  lazy val numCloses = LongAccumulator()
}
