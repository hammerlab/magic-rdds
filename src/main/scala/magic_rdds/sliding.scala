package magic_rdds

import org.hammerlab.magic.rdd.sliding.{ BorrowElems, Sliding }

trait sliding
  extends Sliding
    with BorrowElems

object sliding extends sliding
