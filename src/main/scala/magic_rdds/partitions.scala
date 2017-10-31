package magic_rdds

import org.hammerlab.magic.rdd.partitions._

trait partitions
  extends FilterPartitionIdxs
     with OrderedRepartition
     with PartitionByKey
     with PartitionFirstElems
     with PartitionSizes
     with PrependOrderedIDs
     with CanRangePartition
     with ReducePartitions

object partitions extends partitions
