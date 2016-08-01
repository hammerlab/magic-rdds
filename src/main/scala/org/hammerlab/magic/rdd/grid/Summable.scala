package org.hammerlab.magic.rdd.grid

trait Summable[+Companion <: SummableCompanion[_]] {
  def +(o: Summable[_]): this.type
}

trait SummableCompanion[+S <: Summable[_]] {
  def zero: S
}
