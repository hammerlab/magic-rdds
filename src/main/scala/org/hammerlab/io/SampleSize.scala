package org.hammerlab.io

import caseapp.core.ArgParser
import caseapp.core.ArgParser.instance

case class SampleSize(size: Option[Int]) {
  def <(other: Long): Boolean =
    size.exists(_ < other)
}

object SampleSize {
  implicit val parser: ArgParser[SampleSize] =
    instance("sample size") {
      str ⇒
        Right(
          SampleSize(
            Some(
              str.toInt
            )
          )
        )
    }
}
