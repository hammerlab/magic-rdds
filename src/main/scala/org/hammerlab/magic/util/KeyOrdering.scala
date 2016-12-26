package org.hammerlab.magic.util

/**
 * Order [[Tuple2]]s by key.
 */
class KeyOrdering[T](implicit ordering: Ordering[T]) extends Ordering[(T, _)] {
  override def compare(x: (T, _), y: (T, _)): Int = ordering.compare(x._1, y._1)
}

object KeyOrdering {
  implicit def toKeyOrdering[T](implicit ordering: Ordering[T]): KeyOrdering[T] = new KeyOrdering
}
