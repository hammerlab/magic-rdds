package org.hammerlab.exception

case class Error(t: Throwable) {
  def lines(indent: String = "\t"): List[String] =
    t.toString() ::
      StackTrace(t).lines(indent) ++
        Option(t.getCause)
          .toList
          .flatMap {
            cause ⇒
              "Caused by:" ::
                Error(cause).lines(indent)
          }


  override def toString: String =
    lines().mkString("\n")
}
