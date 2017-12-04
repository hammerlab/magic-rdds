package org.hammerlab.hadoop.splits

import caseapp.core.ArgParser
import caseapp.core.ArgParser.instance
import hammerlab.bytes._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat.SPLIT_MAXSIZE
import org.hammerlab.hadoop.Configuration

case class MaxSplitSize(size: Long)

object MaxSplitSize {

  implicit def maxSplitSizeFromInt (size:   Int): MaxSplitSize = MaxSplitSize(size)
  implicit def maxSplitSizeFromLong(size:  Long): MaxSplitSize = MaxSplitSize(size)
  implicit def maxSplitSizeFromSize(size: Bytes): MaxSplitSize = MaxSplitSize(size)

  implicit def unmakeMaxSplitSize(size: MaxSplitSize): Long = size.size

  val DEFAULT_MAX_SPLIT_SIZE: Bytes = 32 MB

  implicit def apply(size: Option[Bytes] = None)(implicit conf: Configuration): MaxSplitSize =
    size
      .map(MaxSplitSize(_))
      .getOrElse(
        new MaxSplitSize(
          conf.getLong(
            SPLIT_MAXSIZE,
            DEFAULT_MAX_SPLIT_SIZE
          )
        )
      )

  implicit val parseSize: ArgParser[MaxSplitSize] =
    instance("path") {
      bytesStr ⇒
        Right(
          MaxSplitSize(
            Bytes(bytesStr)
          )
        )
    }
}
