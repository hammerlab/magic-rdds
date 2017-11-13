package magic_rdds

import hammerlab.iterator.macros.obj
import org.hammerlab.magic.rdd

/**
 * "Exported" API of the [[org.apache.spark.rdd.RDD]] methods under [[org.hammerlab.magic.rdd]].
 *
 * Each package has a trait that is mixed into the kitchen-sink [[magic_rdds]] object, as well as its own stand-alone
 * object which can be used to only import a relevant subset.
 *
 * Relevant type aliases may also be placed in one or the other, depending on whether it should be exposed via importing
 * magic_rdds._ or only via a sub-package thereof.
 */

@obj trait batch extends rdd.batch

import rdd.cmp._
@obj trait cmp
  extends Equals
     with SameValues {
  val Keyed = rdd.cmp.Keyed
  val Ordered = rdd.cmp.Ordered
  val Unordered = rdd.cmp.Unordered
}

@obj trait collect extends rdd.collect

@obj trait fold extends rdd.fold

import rdd.keyed._
@obj trait keyed
  extends CappedGroupByKey
    with FilterKeys
    with ReduceByKey
    with SampleByKey
    with SplitByKey

@obj trait ordered extends rdd.ordered.SortedRepartition

import rdd.partitions._
@obj trait partitions
  extends FilterPartitionIdxs
     with OrderedRepartition
     with PartitionByKey
     with PartitionFirstElems
     with PartitionSizes
     with PrependOrderedIDs
     with ReducePartitions

@obj trait prefix_sum extends rdd.grid.PrefixSum
object prefix_sum {
  type Result[V] = rdd.grid.Result[V]
  val Result = rdd.grid.Result
}

@obj trait rev extends rdd.rev

@obj trait run_length extends rdd.run_length

@obj trait sample extends rdd.sample

import rdd.scan._
@obj trait scan
  extends ScanLeftRDD
     with ScanLeftValuesRDD
     with ScanRightRDD
     with ScanRightValuesRDD {
  type ScanRDD[T] = rdd.scan.ScanRDD[T]
}

@obj trait size extends rdd.size

import rdd.sliding._
@obj trait sliding
  extends Sliding
     with BorrowElems

@obj trait sort extends rdd.sort

import rdd.zip._
@obj trait zip
  extends ZipPartitions
    with ZipPartitionsWithIndex
