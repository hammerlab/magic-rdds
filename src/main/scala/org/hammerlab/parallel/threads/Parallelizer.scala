package org.hammerlab.parallel.threads

import java.util.concurrent.ConcurrentLinkedDeque

import org.hammerlab.parallel

import scala.collection.concurrent.TrieMap
import scala.reflect.ClassTag

/**
 * Parallel-map over an [[Iterable]] using a configurable [[Config.numThreads]] number of threads
 */
case class Parallelizer[T](input: Iterable[T])(
    implicit
    config: Config
) extends parallel.Parallelizer[T] {
  def pmap[U: ClassTag](fn: T ⇒ U): Array[U] = {

    /** Input queue: elements to process. */
    val queue = new ConcurrentLinkedDeque[(T, Int)]()

    /** Output: map from [input index] to [result], so that original order can be restored. */
    val results = TrieMap[Int, U]()

    /** Enqueue all elements along with their indices, for restoring their order later. */
    input
      .zipWithIndex
      .foreach(queue.add)

    def pollQueue(): Unit = {
      while (true) {
        Option(queue.poll()) match {
          case Some((elem, idx)) ⇒

            val result = fn(elem)
            results(idx) = result
          case _ ⇒
            return
        }
      }
    }

    /** Fork `n-1` worker threads; the current thread will be used as the `n`-th one. */
    val threads =
      for {
        _ ← 0 until (config.numThreads - 1)
      } yield {
        val thread =
          new Thread() {
            override def run(): Unit =
              pollQueue()
          }

        thread.start()
        thread
      }

    /** Use the current thread as the nth worker. */
    pollQueue()

    threads.foreach(_.join())

    results
      .toArray
      .sortBy(_._1)
      .map(_._2)
  }
}
