package org.hammerlab.magic.iterator

/**
 * Given an iterator and a predicate function, emit iterators containing maximal runs of sequential elements that all
 * satisfy the predicate, or individual elements that do not.
 *
 * For example, given an [[Iterator]] containing [[Int]]s [1, 2, 4, 3, 5, 6, 2, 8] and predicate function `_ % 2 == 0`,
 * [[GroupRunsIterator]] would emit [[Iterator]]s containing [1], [2, 4], [3], [5], [6, 2, 8].
 *
 * See GroupRunsIteratorTest for more examples.
 */
class GroupRunsIterator[T](it: BufferedIterator[T], pred: T => Boolean) extends Iterator[Iterator[T]] {
  override def hasNext: Boolean = it.hasNext

  override def next(): Iterator[T] = {
    if (!pred(it.head))
      Iterator(it.next())
    else
      new Iterator[T] {
        override def hasNext: Boolean = it.hasNext && pred(it.head)

        override def next(): T =
          if (!pred(it.head))
            throw new NoSuchElementException
          else
            it.next()
      }
  }
}

object GroupRunsIterator {
  def apply[T](it: Iterable[T], pred: T => Boolean): GroupRunsIterator[T] =
    new GroupRunsIterator[T](it.iterator.buffered, pred)
}
