package org.hammerlab.io

import java.io.PrintStream

import org.hammerlab.hadoop.Path

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
      case SampleSize(Some(size))
        if size < populationSize ⇒
        print(
          truncatedHeader(size),
          samples
            .take(size)
            .mkString(indent, s"\n$indent", ""),
          "…"
        )
      case SampleSize(Some(0)) ⇒
        // No-op
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
}

object Printer {

  implicit def makePrinter(ps: PrintStream): Printer = Printer(ps)
  implicit def unmakePrinter(p: Printer): PrintStream = p.ps

  def apply(file: Option[Path]): Printer =
    file match {
      case Some(file) ⇒
        new PrintStream(file.outputStream)
      case None ⇒
        System.out
    }

  def print(os: Object*)(
      implicit printer: Printer
  ): Unit =
    printer.print(os: _*)

  def printSamples(samples: Seq[_],
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

  def printList(list: Seq[_],
                header: String,
                truncatedHeader: Int ⇒ String,
                indent: String = "\t")(
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
