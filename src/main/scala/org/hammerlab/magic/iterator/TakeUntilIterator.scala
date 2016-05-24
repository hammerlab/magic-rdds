package org.hammerlab.magic.iterator

import scala.collection.mutable.ArrayBuffer

class TakeUntilIterator[T](it: Iterator[T], sentinel: T) extends Iterator[Seq[T]] {
  val buf: ArrayBuffer[T] = ArrayBuffer()
  var idx = 0

  var padded = false
  def fill(throwIfCant: Boolean = false): Unit = {
    buf.clear()
    idx = 0

    if (throwIfCant && !it.hasNext) {
      throw new NoSuchElementException()
    }
    var filling = true
    while (it.hasNext && filling) {
      val next = it.next()
      buf.append(next)
      if (next == sentinel)
        filling = false
    }
    if (!it.hasNext && filling) {
      buf.append(sentinel)
      padded = true
    }
  }

  override def hasNext: Boolean = {
    idx + (if (padded) 1 else 0) < buf.length || it.hasNext
  }

  override def next(): Seq[T] = {
    if (idx == buf.length) {
      fill(true)
    }
    val ret = buf.view(idx, buf.length - 1)
    idx += 1
    ret
  }
}

