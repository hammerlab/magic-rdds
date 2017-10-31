package magic_rdds

import org.hammerlab.magic.rdd.collect.CollectPartitions

trait collect
  extends CollectPartitions

case object collect extends collect
