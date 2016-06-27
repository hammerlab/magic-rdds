package org.hammerlab.magic.iterator

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
