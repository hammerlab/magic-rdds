package org.hammerlab.magic.iterator

/**
 * Interface for implementing [[BufferedIterator]]s following a common pattern: the `hasNext` implementation must
 * actually compute the next element (or return a sentinel that implies that one doesn't exist), and should therefore
 * cache that element to serve `head` and `next()` calls.
 *
 * This interface allows subclasses to simply implement an `_advance` method that returns an [[Option]] containing the
 * next element, if one exists, and [[None]] otherwise, and it takes care of the rest of the boilerplate.
 *
 * It also exposes protected `clear` and `postNext` methods for [[None]]ing the internal state and responding to
 * `next()` having been called, respectively.
 */
trait SimpleBufferedIterator[+T] extends BufferedIterator[T] {

  private[this] var _next: Option[T] = None

  protected final def clear(): Unit = {
    _next = None
  }

  protected def _advance: Option[T]

  override final def hasNext: Boolean = {
    if (_next.isEmpty) {
      _next = _advance
    }
    _next.nonEmpty
  }

  override final def head: T = {
    if (!hasNext) throw new NoSuchElementException
    _next.get
  }

  protected def postNext(): Unit = {}

  override final def next(): T = {
    val r = head
    clear()
    postNext()
    r
  }
}
