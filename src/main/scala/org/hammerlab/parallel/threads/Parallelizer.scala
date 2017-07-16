package org.hammerlab.parallel.threads

import java.lang.Thread.currentThread
import java.util.concurrent.ConcurrentLinkedDeque

import scala.collection.JavaConverters._
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

  override def parallelMap[Ctx, U: ClassTag](init: ⇒ Ctx,
                                             fn: (Ctx, T) ⇒ U,
                                             finish: (Ctx) ⇒ Unit): Array[U] = {
    val numThreads =
      config.numThreads match {
        case n if n > 0 ⇒ n
        case n ⇒
          throw new IllegalArgumentException(
            s"Invalid number of threads: $n"
          )
      }

    /** Input queue: elements to process. */
    val queue = new ConcurrentLinkedDeque[(T, Int)]()

    /** Output: map from [input index] to [result], so that original order can be restored. */
    val results = TrieMap[Int, U]()

    /** Enqueue all elements along with their indices, for restoring their order later. */
    input
      .zipWithIndex
      .foreach(queue.add)

    val exceptions = new ConcurrentLinkedDeque[ParallelWorkerException[T]]()

    def pollQueue(): Unit = {
      val ctx = init
      while (true) {
        Option(queue.poll()) match {
          case Some((elem, idx)) ⇒
            try {
              val result = fn(ctx, elem)
              results(idx) = result
            } catch {
              case e: Throwable ⇒
                exceptions.add(
                  ParallelWorkerException(
                    elem,
                    idx,
                    e
                  )
                )
            }
          case _ ⇒
            finish(ctx)
            return
        }
      }
    }

    /** Fork `n-1` worker threads; the current thread will be used as the `n`-th one. */
    val threads =
      for {
        idx ← 0 until (numThreads - 1)
      } yield {
        val thread =
          new Thread() {
            override def run(): Unit =
              pollQueue()
          }

        thread.setName(s"Worker $idx")
        thread.start()
        thread
      }

    /** Use the current thread as the nth worker. */

    val previousName = currentThread().getName
    currentThread().setName(s"Worker $numThreads")

    pollQueue()

    currentThread().setName(previousName)

    threads.foreach(_.join())

    if (!exceptions.isEmpty)
      throw ParallelWorkerExceptions[T](
        exceptions
          .iterator()
          .asScala
          .toVector
          .sortBy(_.idx)
      )

    results
      .toArray
      .sortBy(_._1)
      .map(_._2)
  }
}
