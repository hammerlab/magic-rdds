package org.hammerlab.magic.rdd

import RunLengthRDD._
import org.hammerlab.spark.test.rdd.Util.makeRDD
import org.hammerlab.spark.test.suite.SparkSuite

class RunLengthRDDTest extends SparkSuite {

  def check(partitionStrs: Iterable[Char]*)(expected: String) = {
    val rle = makeRDD(partitionStrs: _*).runLengthEncode.collect

    val str =
      (for {
        (ch, num) <- rle
      } yield
        s"$num*$ch"
        ).mkString(" ")

    str should ===(expected)
  }

  test("simple") {
    check(
      "000001111000110",
      "00000",
      "11111",
      "10101010"
    )(
      "5*0 4*1 3*0 2*1 6*0 6*1 1*0 1*1 1*0 1*1 1*0 1*1 1*0"
    )
  }

  test("singletons") {
    check(
      "00000",
      "00000",
      "00000",
      "11111",
      "11111",
      "00000"
    )(
      "15*0 10*1 5*0"
    )
  }

  test("one partition") {
    check(
      "00000"
    )(
      "5*0"
    )
  }

  test("no singletons") {
    check(
      "01",
      "10",
      "01",
      "10"
    )(
      "1*0 2*1 2*0 2*1 1*0"
    )
  }

  test("empty partitions") {
    check(
      "000111",
      "",
      "",
      "111"
    )(
      "3*0 6*1"
    )
  }

  test("empty start") {
    check(
      "",
      "",
      "000",
      "000111",
      "111",
      ""
    )(
      "6*0 6*1"
    )
  }
}
