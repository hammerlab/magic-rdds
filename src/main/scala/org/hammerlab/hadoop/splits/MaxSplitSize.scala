package org.hammerlab.hadoop.splits

import caseapp.core.argparser._
import hammerlab.bytes._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat.SPLIT_MAXSIZE
import org.hammerlab.hadoop.Configuration

case class MaxSplitSize(size: Long)

object MaxSplitSize {

  implicit def maxSplitSizeFromInt (size:   Int): MaxSplitSize = MaxSplitSize(size)
  implicit def maxSplitSizeFromLong(size:  Long): MaxSplitSize = MaxSplitSize(size)

  implicit def unmakeMaxSplitSize(size: MaxSplitSize): Long = size.size

  val DEFAULT_MAX_SPLIT_SIZE: Bytes = 32 MB

  def apply(size: Option[Bytes])(implicit conf: Configuration): MaxSplitSize =
    size
      .map(MaxSplitSize(_))
      .getOrElse(MaxSplitSize())

  implicit def apply(size: Bytes): MaxSplitSize = MaxSplitSize(size.bytes)

  implicit def fromConf(implicit conf: Configuration): MaxSplitSize = apply()(conf)

  /**
   * Implicitly create a [[MaxSplitSize]] from a [[Configuration]]; generally used as a fall-back if a [[MaxSplitSize]]
   * is needed and not more explicitly provided
   */
  def apply()(implicit conf: Configuration): MaxSplitSize =
    new MaxSplitSize(
      conf.getLong(
        SPLIT_MAXSIZE,
        DEFAULT_MAX_SPLIT_SIZE
      )
    )

  implicit val parseSize: ArgParser[MaxSplitSize] =
    SimpleArgParser.from("path") {
      bytesStr ⇒
        Right(
          MaxSplitSize(
            Bytes(bytesStr)
          )
        )
    }
}
