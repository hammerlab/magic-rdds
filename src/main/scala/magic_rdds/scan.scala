package magic_rdds

import org.hammerlab.magic.rdd.scan.{ ScanLeftRDD, ScanLeftValuesRDD, ScanRightRDD, ScanRightValuesRDD }

trait scan
  extends ScanLeftRDD
    with ScanLeftValuesRDD
    with ScanRightRDD
    with ScanRightValuesRDD

object scan extends scan
