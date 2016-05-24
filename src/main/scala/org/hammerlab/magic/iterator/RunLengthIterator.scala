package org.hammerlab.magic.iterator

class RunLengthIterator[T] private(it: BufferedIterator[T]) extends Iterator[(T, Int)] {

  override def hasNext: Boolean = it.hasNext

  override def next(): (T, Int) = {
    val elem = it.head
    var count = 0
    while (it.hasNext && it.head == elem) {
      it.next()
      count += 1
    }
    (elem, count)
  }
}

object RunLengthIterator {
  def apply[T](it: Iterator[T]): RunLengthIterator[T] = new RunLengthIterator(it.buffered)

  def reencode[T](it: BufferedIterator[(T, Int)]): Iterator[(T, Int)] = new Iterator[(T, Int)] {
    override def hasNext: Boolean = it.hasNext

    override def next(): (T, Int) = {
      var ret = it.head
      while (it.hasNext && it.head._1 == ret._1) {
        ret = (ret._1, ret._2 + it.next()._2)
      }
      ret
    }
  }
}
