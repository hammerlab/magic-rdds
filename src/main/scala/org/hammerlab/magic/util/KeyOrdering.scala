package org.hammerlab.magic.util

class KeyOrdering[T, U](ordering: Ordering[T]) extends Ordering[(T, U)] {
  override def compare(x: (T, U), y: (T, U)): Int = ordering.compare(x._1, y._1)
}

object KeyOrdering {
  implicit def toKeyOrdering[T, U](ordering: Ordering[T]): KeyOrdering[T, U] = new KeyOrdering(ordering)
}
