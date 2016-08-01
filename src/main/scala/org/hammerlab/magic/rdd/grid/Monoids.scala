package org.hammerlab.magic.rdd.grid

import spire.algebra.Monoid

object Monoids {

  implicit case object MonoidInt extends Monoid[Int] {
    override def id: Int = 0

    override def op(x: Int, y: Int): Int = x + y
  }

}
