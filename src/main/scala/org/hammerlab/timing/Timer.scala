package org.hammerlab.timing

import java.lang.System.currentTimeMillis

import grizzled.slf4j.Logging

trait Timer {
  self: Logging ⇒

  def time[T](fn: ⇒ T): T = time("")(fn)

  def time[T](msg: ⇒ String)(fn: ⇒ T): T = {
    val before = currentTimeMillis
    val res = fn
    val after = currentTimeMillis
    info(s"$msg:\t${after-before}ms")
    res
  }
}
