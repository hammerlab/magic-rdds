package org.hammerlab.magic.iterator

/**
 * Given an [[Iterator[T]]], emit each element sandwiched between its preceding and succeeding elements.
 */
case class Sliding3OptIterator[T](it: BufferedIterator[T])
  extends SimpleBufferedIterator[(Option[T], T, Option[T])] {

  private var lastOpt: Option[T] = None

  override protected def _advance: Option[(Option[T], T, Option[T])] = {
    if (!it.hasNext)
      return None

    val cur = it.next()

    val prevOpt = lastOpt
    lastOpt = Some(cur)

    val nextOpt =
      if (it.hasNext)
        Some(it.head)
      else
        None

    Some(
      (
        prevOpt,
        cur,
        nextOpt
      )
    )
  }
}
