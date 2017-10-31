package org.hammerlab.magic.rdd

import magic_rdds.batch._
import org.apache.spark.HashPartitioner
import org.apache.spark.batch.{ MapRDD, ReduceRDD }
import org.hammerlab.spark.test.suite.SparkSuite

class BatchTest
  extends SparkSuite {

  test("invalid batch size <= 0") {
    val rdd = sc.parallelize(0 until 10, 8)
    var err = intercept[IllegalArgumentException] { rdd.batch(-1) }
    assert(err.getMessage.contains("Positive number of partitions per batch is required"))

    err = intercept[IllegalArgumentException] { rdd.batch(0) }
    assert(err.getMessage.contains("Positive number of partitions per batch is required"))
  }

  test("return original RDD, if it is empty") {
    val rdd = sc.emptyRDD[Int]
    val res = rdd.batch(10)
    res should be (rdd)
  }

  test("return original RDD, if it has fewer partitions than batch size") {
    val rdd = sc.parallelize(0 until 10, 8)
    val res1 = rdd.batch(10)
    res1 should be (rdd)
    val res2 = rdd.batch(8)
    res2 should be (rdd)
  }

  test("return batch reduce RDD, if number of partitions is larger than batch size") {
    val rdd = sc.parallelize(0 until 10, 8)
    val res1 = rdd.batch(4)
    res1.isInstanceOf[ReduceRDD[_]] should be (true)
    // we do allow to use batch size of 1 - one partition per stage
    val res2 = rdd.batch(1)
    res2.isInstanceOf[ReduceRDD[_]] should be (true)
  }

  test("batch map RDD should have same hash partitioner of original partitions") {
    val rdd = sc.parallelize(0 until 10, 8)
    val batch1 = new MapRDD(rdd, None, rdd.partitions)
    batch1.part.isInstanceOf[HashPartitioner] should be (true)
    val batch2 = new MapRDD(rdd, Some(batch1), rdd.partitions)
    batch2.part should be (batch1.part)
  }

  test("batch map RDD should return correct number of dependencies") {
    val rdd = sc.parallelize(0 until 10, 8)
    val batch1 = new MapRDD(rdd, None, rdd.partitions)
    val batch2 = new MapRDD(rdd, Some(batch1), rdd.partitions)
    val batch3 = new MapRDD(rdd, Some(batch2), rdd.partitions)
    batch1.getBatchDependencies should be (Seq.empty)
    batch2.getBatchDependencies should be (Seq(batch1))
    batch3.getBatchDependencies should be (Seq(batch1, batch2))
  }

  test("batch reduce RDD should fail if original partitions are reused") {
    val rdd = sc.parallelize(0 until 10, 8)
    val batch1 = new MapRDD(rdd, None, rdd.partitions)
    val batch2 = new MapRDD(rdd, Some(batch1), rdd.partitions)
    val err = intercept[IllegalStateException] {
      new ReduceRDD(batch2)
    }
    assert(err.getMessage.contains(s"Map-side RDD ${batch2.id} contains duplicate partition"))
  }

  test("batch reduce RDD should fail if original partitions are different from partition map") {
    val rdd = sc.parallelize(0 until 10, 8)
    val batch1 = new MapRDD(rdd, None, rdd.partitions.take(3))
    val batch2 = new MapRDD(rdd, Some(batch1), rdd.partitions.drop(4))
    val err = intercept[IllegalStateException] {
      new ReduceRDD(batch2)
    }
    assert(err.getMessage.contains(
      "Partition-dependency map has 7 partitions, but RDD should have 8 partitions"))
  }

  test("batch reduce RDD should return same partitions as original RDD") {
    val rdd = sc.parallelize(0 until 10, 8)
    val batch1 = new MapRDD(rdd, None, rdd.partitions.take(3))
    val batch2 = new MapRDD(rdd, Some(batch1), rdd.partitions.drop(3))
    val res = new ReduceRDD(batch2)
    res.partitions should be (rdd.partitions)
  }

  test("compute correctness test - no-parent int RDD") {
    val rdd = sc.parallelize(0 until 10, 10)
    val res = rdd.batch(3)
    res.glom.collect should be (rdd.glom.collect)
  }

  test("compute correctness test - no-parent char RDD") {
    val rdd = sc.parallelize(Seq("a", "b", "c", "d", "e", "f", "g", "h"), 10)
    val res = rdd.batch(4)
    res.glom.collect should be (rdd.glom.collect)
  }

  test("compute correctness test - no-parent complex type RDD") {
    val rdd = sc.parallelize(
      Seq((1, true), (2, false), (3, true), (4, false), (5, true), (6, false)), 8)
    val res = rdd.batch(3)
    res.glom.collect should be (rdd.glom.collect)
  }

  test("compute correctness test - one-parent int RDD") {
    val rdd = sc.parallelize(0 until 100, 10).map { x ⇒ x * x }.filter { _ % 2 == 0 }
    val res = rdd.batch(7)
    res.glom.collect should be (rdd.glom.collect)
  }

  test("compute correctness test - shuffle-parent int RDD") {
    val rdd = sc.parallelize(0 until 100, 10).repartition(20)
    val res = rdd.batch(7)
    res.glom.collect should be (rdd.glom.collect)
  }
}
