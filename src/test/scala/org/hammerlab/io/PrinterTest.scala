package org.hammerlab.io

import org.hammerlab.io.Printer._
import org.hammerlab.test.Suite

class PrinterTest
  extends Suite {

  def check(implicit printLimit: SampleSize, expected: String): Unit = {
    val path = tmpPath()
    implicit val printer = Printer(path)

    val list = 1 to 10

    print(
      list,
      "Integers:",
      n ⇒ s"First $n of ${list.size} integers:"
    )

    printer.close()

    path.read should be(expected.stripMargin)
  }

  test("untruncated list") {
    check(
      SampleSize(None),
      """Integers:
        |	1
        |	2
        |	3
        |	4
        |	5
        |	6
        |	7
        |	8
        |	9
        |	10
        |"""
    )
  }

  test("barely not truncated") {
    check(
      SampleSize(9),
      """Integers:
        |	1
        |	2
        |	3
        |	4
        |	5
        |	6
        |	7
        |	8
        |	9
        |	10
        |"""
    )
  }

  test("barely truncated") {
    check(
      SampleSize(8),
      """First 8 of 10 integers:
        |	1
        |	2
        |	3
        |	4
        |	5
        |	6
        |	7
        |	8
        |	…
        |"""
    )
  }

  test("half truncated") {
    check(
      SampleSize(5),
      """First 5 of 10 integers:
        |	1
        |	2
        |	3
        |	4
        |	5
        |	…
        |"""
    )
  }

  test("one") {
    check(
      SampleSize(1),
      """First 1 of 10 integers:
        |	1
        |	…
        |"""
    )
  }

  test("none") {
    check(
      SampleSize(0),
      ""
    )
  }
}
