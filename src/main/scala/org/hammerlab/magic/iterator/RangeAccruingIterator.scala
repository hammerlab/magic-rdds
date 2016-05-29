package org.hammerlab.magic.iterator

class RangeAccruingIterator(it: Iterator[Int]) extends Iterator[Range] {

  var anchor = -1

  override def hasNext: Boolean = it.hasNext || anchor >= 0

  override def next(): Range = {
    if (anchor < 0) anchor = it.next()

    var start = anchor
    var end = anchor + 1
    var continue = true
    while (it.hasNext && continue) {
      val e = it.next()
      if (e == end)
        end += 1
      else {
        continue = false
        anchor = e
      }
    }

    if (continue)
      anchor = -1

    start until end
  }
}
