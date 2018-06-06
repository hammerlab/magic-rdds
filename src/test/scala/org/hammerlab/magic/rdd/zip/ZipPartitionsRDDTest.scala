package org.hammerlab.magic.rdd.zip

import magic_rdds.collect._
import magic_rdds.zip._
import org.hammerlab.cmp.CanEq
import org.hammerlab.spark.test.suite.SparkSuite
import org.hammerlab.test.Cmp

class ZipPartitionsRDDTest
  extends SparkSuite {

  import scala.collection.immutable.IndexedSeq
  implicit def cmpNestedColls[T: Cmp] = CanEq.by[Array[T], IndexedSeq[T]](_.toIndexedSeq)

  lazy val fourPartitions = sc.parallelize(1 to 10, 4)
  lazy val fourPartitions2 = sc.parallelize(10 to 20, 4)
  lazy val fourPartitions3 = sc.parallelize(20 to 30, 4)

  lazy val fivePartitions = sc.parallelize(1 to 10, 5)
  lazy val fivePartitions2 = sc.parallelize(10 to 20, 5)

  test("wrong number of partitions 2") {

    // Regular Spark API doesn't error
    fourPartitions.zipPartitions(fivePartitions) { _ ++ _ }

    // This package's does
    intercept[ZipRDDDifferingPartitionsException] {
      fourPartitions.zippartitions(fivePartitions) { _ ++ _ }
    }
    .rdds should be(
      List(
        fourPartitions,
        fivePartitions
      )
    )
  }

  test("wrong number of partitions 3") {

    // Regular Spark API doesn't error
    fourPartitions.zipPartitions(fourPartitions2, fivePartitions) { _ ++ _ ++ _ }

    // This package's does
    intercept[ZipRDDDifferingPartitionsException] {
      fourPartitions.zippartitions(fourPartitions2, fivePartitions) { _ ++ _ ++ _ }
    }
    .rdds should be(
      List(
        fourPartitions,
        fourPartitions2,
        fivePartitions
      )
    )
  }

  test("zip 2") {
    ==(
      fourPartitions
        .zippartitions(
          fourPartitions2
        ) {
          _ ++ _
        }
        .collectParts,
      Array(
        (1 to  2) ++ (10 to 11),
        (3 to  5) ++ (12 to 14),
        (6 to  7) ++ (15 to 17),
        (8 to 10) ++ (18 to 20)
      )
    )
  }

  test("zip 3") {
    ==(
      fourPartitions
        .zippartitions(
          fourPartitions2,
          fourPartitions3
        ) {
          _ ++ _ ++ _
        }
        .collectParts,
      Array(
        (1 to  2) ++ (10 to 11) ++ (20 to 21),
        (3 to  5) ++ (12 to 14) ++ (22 to 24),
        (6 to  7) ++ (15 to 17) ++ (25 to 27),
        (8 to 10) ++ (18 to 20) ++ (28 to 30)
      )
    )
  }
}
