package org.hammerlab.timing

abstract class StoppableThread
  extends Thread {
  protected var stopped = false
  def end(): Unit = {
    stopped = true
  }
}

