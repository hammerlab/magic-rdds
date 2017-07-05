package org.hammerlab.io

import java.io.PrintStream

import org.hammerlab.paths.Path

case class Printer(ps: PrintStream) {
  def print(os: Object*): Unit =
    for { o ← os } {
      ps.println(o)
    }

  def printSamples(samples: Seq[_],
                   populationSize: Long,
                   header: String,
                   truncatedHeader: Int ⇒ String,
                   indent: String = "\t")(
                      implicit
                      sampleSize: SampleSize
                  ): Unit = {
    sampleSize match {
      case SampleSize(Some(0)) ⇒
        // No-op
      case SampleSize(Some(size))
        if size + 1 < populationSize ⇒
        print(
          truncatedHeader(size),
          samples
            .take(size)
            .mkString(indent, s"\n$indent", ""),
          s"$indent…"
        )
      case _ ⇒
        print(
          header,
          samples.mkString(indent, s"\n$indent", "")
        )
    }
  }

  def printList(list: Seq[_],
                header: String,
                truncatedHeader: Int ⇒ String,
                indent: String = "\t")(
  implicit sampleSize: SampleSize
  ): Unit =
    printSamples(
      list,
      list.size,
      header,
      truncatedHeader,
      indent
    )

  def close(): Unit = ps.close()
}

object Printer {

  implicit def makePrinter(ps: PrintStream): Printer = Printer(ps)
  implicit def unmakePrinter(p: Printer): PrintStream = p.ps

  def apply(path: Path): Printer = apply(Some(path))

  def apply(path: Option[Path]): Printer =
    path match {
      case Some(path) ⇒
        new PrintStream(path.outputStream)
      case None ⇒
        System.out
    }

  /**
   * Named to avoid overloading [[Predef.print]]
   */
  def echo(os: Object*)(
      implicit printer: Printer
  ): Unit =
    printer.print(os: _*)

  def print(samples: Seq[_],
            populationSize: Long,
            header: String,
            truncatedHeader: Int ⇒ String,
            indent: String = "\t")(
      implicit
      printer: Printer,
      sampleSize: SampleSize
  ): Unit =
    printer.printSamples(
      samples,
      populationSize,
      header,
      truncatedHeader,
      indent
    )

  def print(list: Seq[_],
            header: String,
            truncatedHeader: Int ⇒ String)(
      implicit
      printer: Printer,
      sampleSize: SampleSize
  ): Unit =
    print(
      list,
      header,
      truncatedHeader,
      indent = "\t"
    )

  def print(list: Seq[_],
            header: String,
            truncatedHeader: Int ⇒ String,
            indent: String)(
      implicit
      printer: Printer,
      sampleSize: SampleSize
  ): Unit =
    printer.printList(
      list,
      header,
      truncatedHeader,
      indent
    )

  def sampleString(sampledLines: Seq[String], total: Long, indent: String = "\t"): String =
    sampledLines
      .mkString(
        indent,
        s"\n$indent",
        if (sampledLines.size < total)
          "\n$indent…"
        else
          ""
      )
}
