
# Magic RDDs
[![Join the chat at https://gitter.im/hammerlab/magic-rdds](https://badges.gitter.im/hammerlab/magic-rdds.svg)](https://gitter.im/hammerlab/magic-rdds?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Build Status](https://travis-ci.org/hammerlab/magic-rdds.svg?branch=master)](https://travis-ci.org/hammerlab/magic-rdds)
[![Coverage Status](https://coveralls.io/repos/github/hammerlab/magic-rdds/badge.svg?branch=master)](https://coveralls.io/github/hammerlab/magic-rdds?branch=master)
[![Maven Central](https://img.shields.io/maven-central/v/org.hammerlab/magic-rdds_2.11.svg?maxAge=600)](http://search.maven.org/#search%7Cga%7C1%7Ca%3A%22magic-rdds%22)

Enrichment methods for [Apache Spark RDDs](http://spark.apache.org/docs/latest/programming-guide.html#resilient-distributed-datasets-rdds):

```scala
$ spark-shell --packages org.hammerlab:magic-rdds_2.11:4.0.0
…
scala> import magic_rdds._
scala> sc.parallelize(List(1, 1, 1, 2, 2, 2, 2, 2, 2, 10)).runLengthEncode.collect()
res0: Array[(Int, Int)] = Array((1,3), (2,6), (10,1))
```

## Using

Use these Maven coordinates to depend on `magic-rdds`' latest Scala 2.11 build:

```
<dependency>
  <groupId>org.hammerlab</groupId>
  <artifactId>magic-rdds_2.11</artifactId>
  <version>4.0.0</version>
</dependency>
```

In SBT, use:

```
"org.hammerlab" %% "magic-rdds" % "4.0.0"
```

## Overview
Following are explanations of some of the RDDs provided by this repo and the functionality they provide:

### RDDs
RDD-helpers found in [the `org.hammerlab.magic.rdd` package](https://github.com/hammerlab/magic-rdds/tree/master/src/main/scala/org/hammerlab/magic/rdd).

#### [Run-length encoding](https://github.com/hammerlab/magic-rdds/blob/master/src/main/scala/org/hammerlab/magic/rdd/run_length.scala)
Exposes a `runLengthEncode` method on RDDs, per the example above.

#### [Scans](https://github.com/hammerlab/magic-rdds/blob/master/src/main/scala/org/hammerlab/magic/rdd/scan)

##### Basic `.scanLeft` / `.scanRight`

```scala
val rdd = sc.parallelize(1 to 10)

rdd.scanLeft(0)(_ + _).collect
// Array(0, 1, 3, 6, 10, 15, 21, 28, 36, 45)

rdd.scanRight(0)(_ + _).collect
// Array(54, 52, 49, 45, 40, 34, 27, 19, 10, 0)
```

Before the `.collect`, these each return a [`ScanRDD`](src/main/scala/org/hammerlab/magic/rdd/scan/ScanRDD.scala), which is a wrapper around the post-scan `RDD`, the total sum, and an array with the first element in each partition, and is automatically implicitly-convertible to its contained `RDD`.

##### Include each element in the partial-sum that replaces it

```scala
rdd.scanLeftInclusive(0)(_ + _).collect
// Array[Int] = Array(1, 3, 6, 10, 15, 21, 28, 36, 45, 55)

rdd.scanRightInclusive(0)(_ + _).collect
// Array(55, 54, 52, 49, 45, 40, 34, 27, 19, 10)
```

##### Use [`cats.Monoid`s](https://typelevel.org/cats/typeclasses/monoid.html)

```scala
import cats.instances.int._

rdd.scanLeft
rdd.scanRight
rdd.scanLeftInclusive
rdd.scanRightInclusive
```

##### Operate on "value"s of key-values pairs


```scala
val pairRDD = sc.parallelize('a' to 'j' zip (1 to 10))

pairRDD.scanLeftValues.collect
// Array((a,0), (b,1), (c,3), (d,6), (e,10), (f,15), (g,21), (h,28), (i,36), (j,45))

pairRDD.scanLeftValuesInclusive.collect
// Array((a,1), (b,3), (c,6), (d,10), (e,15), (f,21), (g,28), (h,36), (i,45), (j,55))

pairRDD.scanRightValues.collect
// Array((a,54), (b,52), (c,49), (d,45), (e,40), (f,34), (g,27), (h,19), (i,10), (j,0))

pairRDD.scanRightValuesInclusive.collect
// Array((a,55), (b,54), (c,52), (d,49), (e,45), (f,40), (g,34), (h,27), (i,19), (j,10))
```

Additionally, `.scanRight` and `.scanRightValues` expose two implementations with performance tradeoffs:

- the default implementation achieves a `scanRight` by sequencing the following operations:
  - `reverse`
  - `scanLeft`
  - `reverse`

- an alternate implementation, enabled by setting `useRDDReversal = false` in the first parameter list, calls `Iterator.scanRight` on each partition at one point, which materializes the entire partition into memory:

  ```scala
  pairRDD.scanRightValues(useRDDReversal = false).collect
  // Array((a,54), (b,52), (c,49), (d,45), (e,40), (f,34), (g,27), (h,19), (i,10), (j,0))
  ```

  This is generally dangerous with Spark RDDs, where the assumption is typically that a partition is larger than the available executor memory, meaning such an operation is likely to cause an OOM.

#### `.reverse`
Reverse the elements in an RDD, optionally preserving (though still inverting) their partitioning:

```scala
sc.parallelize(1 to 10).reverse().collect
res2: Array[Int] = Array(10, 9, 8, 7, 6, 5, 4, 3, 2, 1)
```

#### `reduceByKey` extensions
Self-explanatory: `.maxByKey`, `.minByKey`

#### RDD Comparisons / Diffs

Example setup:

```scala
val rdd1 = sc.parallelize(1 to 10)
val rdd2 = sc.parallelize((1 to 5) ++ (12 to 7 by -1))

import cmp._
```

##### `unorderedCmp`: compare, disregarding order/position of elements:

```scala
val Unordered.Stats(both, onlyA, onlyB) = rdd1.unorderedCmp(rdd2).stats
both = 9   (1 to 5, 7 to 10)
onlyA = 1  (6)
onlyB = 2  (11, 12)
```

##### `orderedCmp`: distinguish between common elements at the same vs. different indices:

```scala
val Ordered.Stats(eq, reordered, onlyA, onlyB) = rdd1.orderedCmp(rdd2).stats
// eq = 6        (1, 2, 3, 4, 5, 9)
// reordered = 3 (7, 8, 10)
// onlyA = 1     (6)
// onlyB = 2     (11, 12)
```

##### `compareByKey`: unordered comparison of values for each key of paired-RDDs:

```scala
import hammerlab.iterator._

val chars10  = ('a' to 'j') zipWithIndex
val chars4   = chars10.take(4)
val chars5_2 = chars10.mapValues(_ + 10).takeEager(5)

val rdd1 = sc.parallelize(chars10  ++ chars4)
val rdd2 = sc.parallelize(chars5_2 ++ chars4)

val Keyed.Stats(eq, extraA, extraB, onlyA, onlyB) = rdd1.compareByKey(rdd2).stats
// eq     = 4 (a →  1, b →  2, c →  3, d →  4)
// extraA = 4 (a →  1, b →  2, c →  3, d →  4; second copy of each)
// extraB = 0 
// onlyA  = 6 (e →  5, f →  6, g →  7, h →  8, i →  9, j →  10)
// onlyB  = 5 (a → 11, b → 12, c → 13, d → 14, e → 15)
```

#### [CollectPartitionsRDD](https://github.com/hammerlab/magic-rdds/blob/master/src/main/scala/org/hammerlab/magic/rdd/partitions/CollectPartitionsRDD.scala)
Exposes one method, for `collect`ing an `RDD` to the driver while keeping elements in their respective partitions:

```scala
import org.hammerlab.magic.rdd.CollectPartitionsRDD._
sc.parallelize(1 to 12).collectParts
// Array(Array(1, 2, 3), Array(4, 5, 6), Array(7, 8, 9), Array(10, 11, 12))
```

#### [BorrowElemsRDD](https://github.com/hammerlab/magic-rdds/blob/master/src/main/scala/org/hammerlab/magic/rdd/sliding/BorrowElemsRDD.scala)
Exposes a variety of methods for shuffling elements between the start of each partition and the end of the previous partition:

```scala
import org.hammerlab.magic.rdd.BorrowElemsRDD._
sc.parallelize(1 to 12).shiftLeft(1).collectPartitions
// Array(Array(1, 2, 3, 4), Array(5, 6, 7), Array(8, 9, 10), Array(11, 12))

sc.parallelize(1 to 12).shiftLeft(2).collectPartitions
// Array(Array(1, 2, 3, 4, 5), Array(6, 7, 8), Array(9, 10, 11), Array(12))

sc.parallelize(1 to 12).copyLeft(1).collectPartitions
// Array(Array(1, 2, 3, 4), Array(4, 5, 6, 7), Array(7, 8, 9, 10), Array(10, 11, 12))
```

#### [rdd.size](https://github.com/hammerlab/magic-rdds/blob/master/src/main/scala/org/hammerlab/magic/rdd/size/package.scala)

Implicit classes in this package:

- expose `.size` on RDDs, which is identical to `.count` but is cached!
- perform optimizations in the presence of [UnionRDDs](https://github.com/apache/spark/blob/v1.6.1/core/src/main/scala/org/apache/spark/rdd/UnionRDD.scala), caching sizes for the union and its components.
- expose `.sizes` and `.total` on sequences and tuples of RDDs, computing their respective sizes in one Spark job.

  Instead of code like:

	```scala
	val count1 = rdd1.count
	val count2 = rdd2.count
	```

	which runs two Spark jobs, you can instead write:

	```scala
	val (count1, count2) = (rdd1, rdd2).sizes
	````

	and save one job.

	`sizes`/`total` integrate optimally with the caching and UnionRDD-optimizations above.

#### [LazyZippedWithIndexRDD](https://github.com/hammerlab/magic-rdds/blob/master/src/main/scala/org/hammerlab/magic/rdd/zip/LazyZippedWithIndexRDD.scala)

Adds `.lazyZipWithIndex`, which is functionally equivalent to [`RDD.zipWithIndex`](https://github.com/apache/spark/blob/v1.6.1/core/src/main/scala/org/apache/spark/rdd/RDD.scala#L1258), but runs the first of the two necessary jobs (computing per-partition sizes and cumulative offsets) lazily, in a manner truer to the spirit of the lazy-wherever-possible RDD API than the `.zipWithIndex` implementation.

#### [SequenceFileSerializableRDD](https://github.com/hammerlab/magic-rdds/blob/master/src/main/scala/org/hammerlab/magic/rdd/serde/SequenceFileSerializableRDD.scala)

`.saveAsSequenceFile` and `.saveCompressed` methods for non-paired RDDs.

#### [SlidingRDD](https://github.com/hammerlab/magic-rdds/blob/master/src/main/scala/org/hammerlab/magic/rdd/sliding/SlidingRDD.scala)

Exposes `.sliding` methods (and several variants) in the spirit of [Scala collections' similar API](https://github.com/scala/scala/blob/v2.10.5/src/library/scala/collection/IterableLike.scala#L164):

```scala
scala> import org.hammerlab.magic.rdd.SlidingRDD._
…
scala> sc.parallelize(0 to 10).sliding2().collect
res1: Array[(Int, Int)] = Array((0,1), (1,2), (2,3), (3,4), (4,5), (5,6), (6,7), (7,8), (8,9), (9,10))
```

#### [CappedGroupByKeyRDD](https://github.com/hammerlab/magic-rdds/blob/master/src/main/scala/org/hammerlab/magic/rdd/keyed/CappedGroupByKeyRDD.scala)

Exposes `.cappedGroupByKey(maxPerKey: Int)`, which is like []`RDD.groupByKey`](https://github.com/apache/spark/blob/v1.6.1/core/src/main/scala/org/apache/spark/rdd/PairRDDFunctions.scala#L631) but helps you not to OOM yourself by only taking the first `maxPerKey` elements for each key!

#### [SampleByKeyRDD](https://github.com/hammerlab/magic-rdds/blob/master/src/main/scala/org/hammerlab/magic/rdd/keyed/SampleByKeyRDD.scala)

Exposes `.sampleByKey`, which functions similarly to `.cappedGroupByKey` above, but samples elements from each key in an unbiased manner.

Powered by a custom [HyperGeometricDistribution](https://github.com/hammerlab/magic-rdds/blob/master/src/main/scala/org/hammerlab/magic/math/HyperGeometricDistribution.scala) implementation that can operate on 8-byte-`Long` population sizes.

#### [SplitByKeyRDD](https://github.com/hammerlab/magic-rdds/blob/master/src/main/scala/org/hammerlab/magic/rdd/keyed/SplitByKeyRDD.scala)

Split an `RDD[(K, V)]` into a `Map[K, RDD[V]]`, i.e. multiple RDDs each containing the values corresponding to one key.

 This is generally a questionable thing to want to do, as subsequent operations on each RDD lose out on Spark's ability to parallelize things.

 However, if you are going to do it, this implementation is much better than what you might do naively, i.e. using `.filter` N times on the original RDD.

 Instead, we shuffle the full RDD once, into a partitioning where each key's pairs occupy a contiguous range of partitions, then partition-slice views over those ranges are exposed as standalone, per-key RDDs.

#### [PartialSumGridRDD](https://github.com/hammerlab/magic-rdds/blob/master/src/main/scala/org/hammerlab/magic/rdd/grid/PartialSumGridRDD.scala)

Given an RDD of elements that each have a logical "row", "column", and "summable" value (an `RDD[((Int, Int), V)]`), generate an RDD that replaces each value with the sum of all values at greater (or equal) rows and columns.

[Examples from the tests](https://github.com/hammerlab/magic-rdds/blob/master/src/test/scala/org/hammerlab/magic/rdd/grid/PartialSumGridRDDTest.scala#L101-L123) should help clarify:

Input:

```
 3   2   1   0    
 7   6   5   4    
11  10   9   8    
15  14  13  12    
```

Output:

```
  6   3   1   0
 28  18  10   4
 66  45  27  12
120  84  52  24
```

### ["Batch" execution](src/main/scala/org/apache/spark/batch)
This feature exposes a coarse avenue for forcing fewer than `spark.executor.cores` tasks to run concurrently on each 
executor, during a given stage.

The stage in question is split up into multiple Spark stages, each comprised of a set
number of partitions from the upstream RDD (the "batch size").

If this size is chosen to be ≤ the number of executors, then in general a maximum of one task will be assigned to each 
executor.

This can be useful when some stages in an app are very memory-expensive, while others are not: the memory-expensive ones 
can be "batched" in this way, each task availing itself of a full executor's-worth of memory. 

The implementation splits the upstream partitions into (N/batch size) batches and executes each batch as a single stage 
before combining the results into single RDD:

```scala
scala> import org.hammerlab.magic.rdd.batch.implicits._
// create RDD of 10 partitions
scala> val rdd = sc.parallelize(0 until 100, 10)
scala> val res = rdd.batch(numPartitionsPerBatch = 4)
// print operations graph to see how many batches are selected
scala> res.toDebugString
res0: String =
(10) ReduceRDD[4] at RDD at ReduceRDD.scala:19 []
 +-(2) MapRDD[3] at RDD at MapRDD.scala:21 []
    +-(4) MapRDD[2] at RDD at MapRDD.scala:21 []
       +-(4) MapRDD[1] at RDD at MapRDD.scala:21 []
          |  ParallelCollectionRDD[0] at parallelize at <console>:27 []
```

See [the package README](src/main/scala/org/apache/spark/batch) for more info!

### And more!
Browse the code and tests, file an issue, or drop by [Gitter](https://gitter.im/hammerlab/magic-rdds) for more info.

## Building
Typical SBT commands will build/test/package the project:

```bash
sbt test
sbt assembly
```

## Releasing
While set to a `-SNAPSHOT` version:

```bash
sbt publish
```

To release a non-`-SNAPSHOT` version:

```bash
sbt publishSigned sonatypeRelease
```
