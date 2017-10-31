package magic_rdds

import org.hammerlab.magic.rdd


trait batch extends rdd.batch
object batch extends batch


trait cmp
  extends rdd.cmp.Equals
     with rdd.cmp.SameValues

object cmp extends cmp


trait collect extends rdd.collect
case object collect extends collect


import rdd.keyed._

trait keyed
  extends CappedGroupByKey
    with FilterKeys
    with ReduceByKey
    with SampleByKey
    with SplitByKey

object keyed extends keyed


import rdd.ordered.SortedRepartition

trait ordered extends SortedRepartition
object ordered extends ordered


import rdd.partitions._

trait partitions
  extends FilterPartitionIdxs
     with OrderedRepartition
     with PartitionByKey
     with PartitionFirstElems
     with PartitionSizes
     with PrependOrderedIDs
     with ReducePartitions

object partitions extends partitions


trait rev extends rdd.rev
object rev extends rev


trait run_length extends rdd.run_length
object run_length extends run_length


trait sample extends rdd.sample
object sample extends sample


import rdd.scan._

trait scan
  extends ScanLeftRDD
     with ScanLeftValuesRDD
     with ScanRightRDD
     with ScanRightValuesRDD

object scan extends scan


trait size extends rdd.size
object size extends size


import rdd.sliding._

trait sliding
  extends Sliding
     with BorrowElems

object sliding extends sliding


trait sort extends rdd.sort
object sort extends sort


import rdd.zip._

trait zip
  extends ZipPartitions
    with ZipPartitionsWithIndex

object zip extends zip
