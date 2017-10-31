package magic_rdds

import org.hammerlab.magic.rdd.zip.{ ZipPartitions, ZipPartitionsWithIndex }

trait zip
  extends ZipPartitions
    with ZipPartitionsWithIndex

object zip extends zip
