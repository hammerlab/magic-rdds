
# Magic RDDs
[![Join the chat at https://gitter.im/hammerlab/magic-rdds](https://badges.gitter.im/hammerlab/magic-rdds.svg)](https://gitter.im/hammerlab/magic-rdds?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Build Status](https://travis-ci.org/hammerlab/magic-rdds.svg?branch=master)](https://travis-ci.org/hammerlab/magic-rdds)
[![Coverage Status](https://coveralls.io/repos/github/hammerlab/magic-rdds/badge.svg?branch=master)](https://coveralls.io/github/hammerlab/magic-rdds?branch=master)

Miscellaneous functionality for manipulating [Apache Spark RDDs](http://spark.apache.org/docs/latest/programming-guide.html#resilient-distributed-datasets-rdds), typically exposed as methods on RDDs via implicit conversions, e.g.:

```scala
$ spark-shell --packages "org.hammerlab:magic-rdds:1.0.0"
…
scala> import org.hammerlab.magic.rdd.RunLengthRDD._
scala> sc.parallelize(List(1, 1, 1, 2, 2, 2, 2, 2, 2, 10)).runLengthEncode.collect()
res0: Array[(Int, Int)] = Array((1,3), (2,6), (10,1))
```

## Using

Use these Maven coordinates to depend on magic-rdds:

```
<dependency>
  <groupId>org.hammerlab</groupId>
  <artifactId>magic-rdds</artifactId>
  <version>1.0.0</version>
</dependency>
```

## Overview
Following are explanations of some of the RDDs provided by this repo and the functionality they provide:

### RDDs
RDD-helpers found in [the `org.hammerlab.magic.rdd` package](https://github.com/hammerlab/magic-rdds/tree/master/src/main/scala/org/hammerlab/magic/rdd).

#### [RunLengthRDD](https://github.com/hammerlab/magic-rdds/blob/master/src/main/scala/org/hammerlab/magic/rdd/RunLengthRDD.scala)
Exposes one method (actually a `lazy val`, so result is cached), `runLengthEncode`, which run-length-encodes the elements of an RDD, per the example above.

#### [ReduceByKeyRDD](https://github.com/hammerlab/magic-rdds/blob/master/src/main/scala/org/hammerlab/magic/rdd/keyed/ReduceByKeyRDD.scala)
Given an `RDD[(K, V)]` and an implicit `Ordering[V]`, provides `maxByKey` and `minByKey` methods.

#### [EqualsRDD](https://github.com/hammerlab/magic-rdds/blob/master/src/main/scala/org/hammerlab/magic/rdd/cmp/EqualsRDD.scala)
Provides methods for "diff"ing two RDDs:

* `compareElements`: perform a `fullOuterJoin` on two RDDs, and return various methods for inspecting the number of elements that are common to both or only found in one or the other.
* `compare`: similar to the above, but requires elements' rank (i.e. index within the RDD) to match.

Example:

```scala
import org.hammerlab.magic.rdd.cmp.EqualsRDD._
import org.hammerlab.magic.rdd.ElemCmpStats

val a = sc.parallelize(1 to 10)
val b = sc.parallelize(15 to 2 by -1)

val stats = a.compareElements(b).stats
// ElemCmpStats(9,1,5)

val ElemCmpStats(both, onlyA, onlyB) = stats
// both: Long = 9
// onlyA: Long = 1
// onlyB: Long = 5
```

#### [SameElementsRDD](https://github.com/hammerlab/magic-rdds/blob/master/src/main/scala/org/hammerlab/magic/rdd/cmp/SameElementsRDD.scala)
Similar to EqualsRDD, but operates on an `RDD[(K, V)]`, joins elements by key, and compares their values.

#### [CollectPartitionsRDD](https://github.com/hammerlab/magic-rdds/blob/master/src/main/scala/org/hammerlab/magic/rdd/partitions/CollectPartitionsRDD.scala)
Exposes one method, for `collect`ing an `RDD` to the driver while keeping elements in their respective partitions:

```scala
import org.hammerlab.magic.rdd.CollectPartitionsRDD._
sc.parallelize(1 to 12).collectPartitions
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

#### [CachedCountRegistry](https://github.com/hammerlab/magic-rdds/blob/master/src/test/scala/org/hammerlab/magic/rdd/CachedCountRegistryTest.scala)

Exposes `.size` on RDDs, which is identical to `.count` but is cached! Also performs optimizations in the presence of [UnionRDDs](https://github.com/apache/spark/blob/v1.6.1/core/src/main/scala/org/apache/spark/rdd/UnionRDD.scala), caching sizes for the union and its components.

Also exposes `.sizes` and `.total` on sequences of RDDs, computing their respective sizes in one Spark job, for added efficiency over code like:

```scala
val count1 = rdd1.count
val count2 = rdd2.count
```

You can instead write:

```scala
val sizes = Seq(rdd1, rdd2).sizes
val count1 = sizes(0)
val count2 = sizes(1)
````

and save yourself a Spark job.

#### [LazyZippedWithIndexRDD](https://github.com/hammerlab/magic-rdds/blob/master/src/main/scala/org/hammerlab/magic/rdd/zip/LazyZippedWithIndexRDD.scala)

Adds `.lazyZipWithIndex`, which is functionally equivalent to [`RDD.zipWithIndex`](https://github.com/apache/spark/blob/v1.6.1/core/src/main/scala/org/apache/spark/rdd/RDD.scala#L1258), but runs the first of the two necessary jobs (computing per-partition sizes and cumulative offsets) lazily, in a manner truer to the spirit of the lazy-wherever-possible RDD API than the `.zipWithIndex` implementation.

#### [SequenceFileSerializableRDD](https://github.com/hammerlab/magic-rdds/blob/master/src/main/scala/org/hammerlab/magic/rdd/serde/SequenceFileSerializableRDD.scala)

`.saveAsSequenceFile` and `.saveCompressed` methods for non-paired RDDs.

#### [SlidingRDD](https://github.com/hammerlab/magic-rdds/blob/master/src/main/scala/org/hammerlab/magic/rdd/sliding/SlidingRDD.scala)

Exposes `.sliding` methods (and several variants) in the spirit of []Scala collections' similar API](https://github.com/scala/scala/blob/v2.10.5/src/library/scala/collection/IterableLike.scala#L164): 

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

### Iterators
[The `iterators` package](https://github.com/hammerlab/magic-rdds/tree/master/src/main/scala/org/hammerlab/magic/iterator) has some handy custom iterators, some necessitated by the RDDs described above, others just there for fun.

### Descriptive Statistics
[`org.hammerlab.magic.stats.Stats`](https://github.com/hammerlab/magic-rdds/blob/master/src/main/scala/org/hammerlab/magic/stats/Stats.scala) has APIs for ingesting numeric elements and outputting nicely formatted statistics about them; modeled after [Apache commons-math `DescriptiveStatistics`](https://github.com/apache/commons-math/blob/MATH_3_6_1/src/main/java/org/apache/commons/math3/stat/descriptive/DescriptiveStatistics.java).

As a bonus, [it can ingest numbers in histogram-style / run-length-encoded format](https://github.com/hammerlab/magic-rdds/blob/master/src/main/scala/org/hammerlab/magic/stats/Stats.scala#L81), supporting `Long` values as well for computations involving element counts from RDDs: 

```scala
scala> import org.hammerlab.magic.stats.Stats
scala> :paste
Stats.fromHist(
    List[(Int, Long)](
        1 ->  10000000000L,
        2 ->   1000000000,
        1 ->          100,
        2 ->   1000000000
    )
)

res0: org.hammerlab.magic.stats.Stats[Int,Long] =
num:   	12000000100,   	mean:  	1.2,   	stddev:	0.4,   	mad:   	0
elems: 	1×10000000000, 2×1000000000, 1×100, 2×1000000000
sorted:	1×10000000100, 2×2000000000
0.0:   	1
0.1:   	1
1:     	1
5:     	1
10:    	1
25:    	1
50:    	1
75:    	1
90:    	2
95:    	2
99:    	2
99.9:  	2
100.0: 	2
```

### Scalatest Utilities
Helpers for:
- giving your tests fresh `SparkContext`s
  - [per-`Suite`](https://github.com/hammerlab/magic-rdds/blob/master/src/main/scala/org/hammerlab/magic/test/spark/SparkSuite.scala) (a thin wrapper around [holdenk/spark-testing-base](https://github.com/holdenk/spark-testing-base)'s [`SharedSparkContext`](https://github.com/holdenk/spark-testing-base/blob/v0.3.3/src/main/1.3/scala/com/holdenkarau/spark/testing/SharedSparkContext.scala))
  - or [per-case](https://github.com/hammerlab/magic-rdds/blob/master/src/main/scala/org/hammerlab/magic/test/spark/PerCaseSuite.scala)
- [making an RDD with specific elements in specific partitions](https://github.com/hammerlab/magic-rdds/blob/master/src/main/scala/org/hammerlab/magic/test/rdd/Util.scala), 
- [creating and cleaning up temporary files and directories](https://github.com/hammerlab/magic-rdds/blob/master/src/main/scala/org/hammerlab/magic/test/TmpFiles.scala)
- [verifying the number of Spark jobs that have been run](https://github.com/hammerlab/magic-rdds/blob/master/src/main/scala/org/apache/spark/scheduler/test/ContextUtil.scala)

### And more!
Browse the code and tests, file an issue, or drop by [Gitter](https://gitter.im/hammerlab/magic-rdds) for more info.
