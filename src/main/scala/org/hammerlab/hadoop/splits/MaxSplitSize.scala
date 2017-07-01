package org.hammerlab.hadoop.splits

import caseapp.core.ArgParser
import caseapp.core.ArgParser.instance
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat.SPLIT_MAXSIZE
import org.hammerlab.hadoop.Configuration
import org.hammerlab.io.Size
import org.hammerlab.io.Size._

case class MaxSplitSize(size: Long)

object MaxSplitSize {
  implicit def makeMaxSplitSize(size: Long): MaxSplitSize = MaxSplitSize(size)
  implicit def unmakeMaxSplitSize(size: MaxSplitSize): Long = size.size

  val DEFAULT_MAX_SPLIT_SIZE: Size = 32 MB

  def apply(size: Option[Long] = None)(implicit conf: Configuration): MaxSplitSize =
    MaxSplitSize(
      size.getOrElse(
        conf.getLong(
          SPLIT_MAXSIZE,
          DEFAULT_MAX_SPLIT_SIZE
        )
      )
    )

  implicit val parseSize: ArgParser[MaxSplitSize] =
    instance("path") {
      bytesStr ⇒ Right(MaxSplitSize(Size(bytesStr)))
    }

  implicit def maxSplitSizeWithDefault(maxSplitSize: Option[MaxSplitSize])(
      implicit conf: Configuration
  ): MaxSplitSize =
    maxSplitSize.getOrElse(MaxSplitSize())
}
