package org.hammerlab.magic.iterator

trait OptionIterator[+T] extends BufferedIterator[T] {

  private[this] var _next: Option[T] = None

  def clear(): Unit = {
    _next = None
  }

  def _advance: Option[T]

  override def hasNext: Boolean = {
    if (_next.isEmpty) {
      _next = _advance
    }
    _next.nonEmpty
  }

  override def head: T = {
    if (!hasNext) throw new NoSuchElementException
    _next.get
  }

  def postNext(): Unit = {}

  override def next(): T = {
    val r = head
    clear()
    postNext()
    r
  }
}
