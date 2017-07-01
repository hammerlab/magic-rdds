package org.hammerlab.timing

object Interval {
  def heartbeat[T](fn: () ⇒ Unit,
                   bodyFn: ⇒ T,
                   intervalS: Int = 1): T = {
    val thread =
      new StoppableThread {
        override def run(): Unit = {
          while (!stopped) {
            Thread.sleep(intervalS * 1000)
            fn()
          }
        }
      }

    thread.start()

    val ret = bodyFn

    thread.end()

    ret
  }
}
