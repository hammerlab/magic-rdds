package org.hammerlab.timing

object Timer {
  def time[T](fn: => T): T = time("")(fn)

  def time[T](msg: ⇒ String)(fn: => T): T = {
    val before = System.currentTimeMillis
    val res = fn
    val after = System.currentTimeMillis
    println(s"$msg:\t${after-before}ms")
    res
  }
}
