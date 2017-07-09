package org.hammerlab.parallel.threads

import org.hammerlab.parallel.makeParallelizer
import org.hammerlab.test.matchers.lines.Line._
import org.hammerlab.test.matchers.lines.LineNumber
import org.hammerlab.test.{ Suite, linesMatch }

abstract class ExceptionTest(implicit config: Config)
  extends Suite {

  /**
   * Parallelize some ints, and throw exceptions on indices 1 and 3
   */
  def make(arr: Array[Int]): Array[String] =
    arr
      .zipWithIndex
      .pmap[String] {
        case (_, 1) ⇒
          throw Index1Exception
        case (elem, 3) ⇒
          throw Index3Exception(elem)
        case (elem, _) ⇒
          elem.toString
      }

  test("except") {
    val exception =
      intercept[ParallelWorkerExceptions[Int]] {
        make(1 to 10 toArray)
      }

    exception
      .exceptions should be(
        Seq(
          ParallelWorkerException((2, 1), 1, Index1Exception),
          ParallelWorkerException((4, 3), 3, Index3Exception(4))
        )
      )

    // Convenience iterator for stepping through lines of the exception's string-representation
    val lines =
      new BufferedIterator[String] {
        var idx = 0
        val lines =
          exception
            .toString
            .split("\n")

        override def head: String = lines(idx)

        override def hasNext: Boolean =
          idx < lines.length

        override def next(): String = {
          val line = head
          idx += 1
          line
        }
      }

    lines
      .take(6)
      .mkString("\n") should linesMatch(
        "org.hammerlab.parallel.threads.ParallelWorkerExceptions: 2 uncaught exceptions thrown in parallel worker threads:",
        "	org.hammerlab.parallel.threads.Index1Exception$: foo",
        "		at org.hammerlab.parallel.threads.Index1Exception$.<clinit>(ExceptionTest.scala:-1)",
        "		at org.hammerlab.parallel.threads.ExceptionTest$$anonfun$make$2.apply(ExceptionTest.scala:" ++ LineNumber ++ ")",
        "		at org.hammerlab.parallel.threads.ExceptionTest$$anonfun$make$2.apply(ExceptionTest.scala:" ++ LineNumber ++ ")",
        "		at org.hammerlab.parallel.threads.Parallelizer.org$hammerlab$parallel$threads$Parallelizer$$pollQueue$1(Parallelizer.scala:" ++ LineNumber ++ ")"
      )

    while (lines.hasNext && lines.head.startsWith("\t\t"))
      lines.next()

    lines
      .take(4)
      .mkString("\n") should linesMatch(
        "	org.hammerlab.parallel.threads.Index3Exception: bar-4",
        "		at org.hammerlab.parallel.threads.ExceptionTest$$anonfun$make$2.apply(ExceptionTest.scala:" ++ LineNumber ++ ")",
        "		at org.hammerlab.parallel.threads.ExceptionTest$$anonfun$make$2.apply(ExceptionTest.scala:" ++ LineNumber ++ ")",
        "		at org.hammerlab.parallel.threads.Parallelizer.org$hammerlab$parallel$threads$Parallelizer$$pollQueue$1(Parallelizer.scala:" ++ LineNumber ++ ")"
      )

    while (lines.hasNext && lines.head.startsWith("\t\t"))
      lines.next()

    lines.hasNext should be(false)
  }
}

class OneThreadException
  extends ExceptionTest()(Config(1))

class TwoThreadsException
  extends ExceptionTest()(Config(2))

case object Index1Exception
  extends Exception("foo")

case class Index3Exception(n: Int)
  extends Exception(s"bar-$n")
