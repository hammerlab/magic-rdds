package org.hammerlab.exception

case class StackTrace(elements: Seq[StackTraceElem]) {
  def lines(indent: String = "\t"): List[String] =
    elements
      .toList
      .map(
        elem ⇒
          s"$indent$elem"
      )

  override def toString: String =
    lines()
      .mkString("\n")
}

object StackTrace {
  def apply(e: Throwable): StackTrace =
    StackTrace(
      e
        .getStackTrace
        .map(
          e ⇒
            e: StackTraceElem
        )
    )
}
