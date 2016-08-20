
# Magic RDDs

[![Join the chat at https://gitter.im/hammerlab/magic-rdds](https://badges.gitter.im/hammerlab/magic-rdds.svg)](https://gitter.im/hammerlab/magic-rdds?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
Miscellaneous functionality for manipulating [Apache Spark RDDs](http://spark.apache.org/docs/latest/programming-guide.html#resilient-distributed-datasets-rdds).

## Installing
Helper functions in this repo are generally available as methods on RDDs via implicit conversions, e.g.:

```scala
$ spark-shell --jars target/magic-rdds-1.0.0-SNAPSHOT.jar
…
scala> import org.hammerlab.magic.rdd.RunLengthRDD._
scala> sc.parallelize(List(1, 1, 1, 2, 2, 2, 2, 2, 2, 10)).runLengthEncode.collect()
res0: Array[(Int, Int)] = Array((1,3), (2,6), (10,1))
```

For now, you can `mvn install` it into your local repo and depend on it from other projects as:

```
<dependency>
  <groupId>org.hammerlab</groupId>
  <artifactId>magic-rdds</artifactId>
  <version>1.0.0-SNAPSHOT</version>
</dependency>
```

## Overview
Following are explanations of some of the RDDs provided by this repo and the functionality they provide:

### RunLengthRDD
Exposes one method (actually a `lazy val`, so result is cached), `runLengthEncode`, which run-length-encodes the elements of an RDD, per the example above.

### ReduceByKeyRDD
Given an `RDD[(K, V)]` and an implicit `Ordering[V]`, provides `maxByKey` and `minByKey` methods.

### EqualsRDD
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

### SameElementsRDD
Similar to EqualsRDD, but operates on an `RDD[(K, V)]`, joins elements by key, and compares their values.

### CollectPartitionsRDD
Exposes one method, for `collect`ing an `RDD` to the driver while keeping elements in their respective partitions:

```scala
import org.hammerlab.magic.rdd.CollectPartitionsRDD._
sc.parallelize(1 to 12).collectPartitions
// Array(Array(1, 2, 3), Array(4, 5, 6), Array(7, 8, 9), Array(10, 11, 12))
```

### BorrowElemsRDD
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

### And more!
Browse the code and tests, or file an issue, for more info.
