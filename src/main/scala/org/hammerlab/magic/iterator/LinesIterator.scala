package org.hammerlab.magic.iterator

import java.io.{BufferedReader, InputStream, InputStreamReader}

class LinesIterator(br: BufferedReader) extends SimpleBufferedIterator[String] {
  override def _advance: Option[String] =
    if (br.ready)
      Some(br.readLine())
    else
      None
}

object LinesIterator {
  def apply(is: InputStream): LinesIterator =
    new LinesIterator(
      new BufferedReader(
        new InputStreamReader(
          is
        )
      )
    )
}
