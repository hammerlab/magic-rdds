package org.hammerlab.timing

import grizzled.slf4j.Logging

trait Timer {
  self: Logging ⇒

  def time[T](fn: => T): T = time("")(fn)

  def time[T](msg: ⇒ String)(fn: => T): T = {
    val before = System.currentTimeMillis
    val res = fn
    val after = System.currentTimeMillis
    info(s"$msg:\t${after-before}ms")
    res
  }
}
