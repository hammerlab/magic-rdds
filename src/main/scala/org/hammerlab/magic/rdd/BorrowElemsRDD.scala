package org.hammerlab.magic.rdd

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

import org.apache.spark.zip.ZipPartitionsWithIndexRDD._

class BorrowElemsRDD[T: ClassTag](@transient rdd: RDD[T]) extends Serializable {

  def shift(n: Int, fill: T): RDD[T] = shift(n, Some(fill))
  def shift(n: Int, partitionOverrides: Map[Int, Int], allowIncompletePartitions: Boolean): RDD[T] = shift(n, None, partitionOverrides, allowIncompletePartitions)
  def shift(n: Int, fill: T, partitionOverrides: Map[Int, Int]): RDD[T] = shift(n, Some(fill), partitionOverrides)
  def shift(n: Int, fillOpt: Option[T] = None, partitionOverrides: Map[Int, Int] = Map(), allowIncompletePartitions: Boolean = false): RDD[T] = {
    copyN(
      n,
      (partitionIdx: Int, it: Iterator[T], tail: Iterator[T]) =>
        (
          if (partitionIdx == 0)
            it
          else
            it.drop(n)
        ) ++ tail,
      fillOpt,
      partitionOverrides,
      allowIncompletePartitions
    )
  }

  def copy(n: Int, fill: T): RDD[T] = copy(n, Some(fill))
  def copy(n: Int, fill: T, partitionOverrides: Map[Int, Int]): RDD[T] = copy(n, Some(fill), partitionOverrides)
  def copy(n: Int, fillOpt: Option[T] = None, partitionOverrides: Map[Int, Int] = Map()): RDD[T] = {
    copyN(
      n,
      (_: Int, it: Iterator[T], tail: Iterator[T]) => it ++ tail,
      fillOpt,
      partitionOverrides
    )
  }

  def copyN(n: Int,
            fn: (Int, Iterator[T], Iterator[T]) => Iterator[T],
            fill: T,
            partitionOverrides: Map[Int, Int]): RDD[T] =
    copyN(n, fn, Some(fill), partitionOverrides)

  def copyN(n: Int,
            fn: (Int, Iterator[T], Iterator[T]) => Iterator[T],
            fillOpt: Option[T] = None,
            partitionOverrides: Map[Int, Int] = Map(),
            allowIncompletePartitions: Boolean = false): RDD[T] = {
    val N = rdd.getNumPartitions

    val partitionOverridesBroadcast = rdd.sparkContext.broadcast(partitionOverrides)

    val copiedElemsRDD: RDD[T] =
      rdd
        .mapPartitionsWithIndex((partitionIdx, iter) =>
          if (partitionIdx == 0)
            fillOpt.toSeq.flatMap(fill =>
              (0 until n).map(i => (N - 1, 0, i) -> fill)
            ).toIterator
          else {
            val copiedElems =
              if (allowIncompletePartitions)
                iter.take(n)
              else {
                val copiedElemsArr = iter.take(n).toArray
                if (copiedElemsArr.length < n) {
                  throw new NoSuchElementException(
                    s"Found ${copiedElemsArr.length} elements in partition $partitionIdx; needed ≥ $n"
                  )
                }
                copiedElemsArr.iterator
              }

            // By default, send elements one partition to "the left".
            val sendToIdx = partitionOverridesBroadcast.value.getOrElse(partitionIdx, partitionIdx - 1)
            for {
              (elem, idx) <- copiedElems.zipWithIndex
            } yield
              (sendToIdx, partitionIdx, idx) → elem
          }
        )
        .repartitionAndSortWithinPartitions(new KeyPartitioner(N))
        .values

    rdd.zipPartitionsWithIndex(copiedElemsRDD)((partitionIdx, iter, tailIter) ⇒
      fn(partitionIdx, iter, tailIter)
    )
  }

  def shift(fn: Iterator[T] => Iterator[T]): RDD[T] = {
    val shiftedElemsRDD =
      rdd
        .mapPartitionsWithIndex((partitionIdx, iter) =>
          if (partitionIdx == 0)
            Iterator()
          else
            for {
              (elem, idx) <- fn(iter).zipWithIndex
            } yield
              (partitionIdx - 1, idx) -> elem
        )
        .repartitionAndSortWithinPartitions(KeyPartitioner(rdd))
        .values

    val numShiftedElemsRDD =
      rdd
        .mapPartitionsWithIndex((partitionIdx, iter) =>
          if (partitionIdx == 0)
            Iterator()
          else
            Iterator(partitionIdx -> fn(iter).size)
        )
        .partitionBy(KeyPartitioner(rdd))
        .values

    rdd.zipPartitions(numShiftedElemsRDD, shiftedElemsRDD)((elems, numToDropIter, newElemsIter) => {

      elems.drop(
        if (numToDropIter.hasNext)
          numToDropIter.next()
        else
          0
      ) ++ newElemsIter
    })
  }

  def copyFirstElems(fn: Iterator[T] => Iterator[T]): RDD[T] = {
    val firstElemsRDD =
      rdd
        .mapPartitionsWithIndex((partitionIdx, iter) =>
          if (partitionIdx == 0)
            Iterator()
          else
            for {
              (elem, idx) <- fn(iter).zipWithIndex
            } yield
              (partitionIdx - 1, idx) -> elem
        )
        .repartitionAndSortWithinPartitions(KeyPartitioner(rdd)).values

    rdd.zipPartitions(firstElemsRDD)(_ ++ _)
  }

  def shiftPartitions[U: ClassTag]: RDD[T] =
    rdd
      .mapPartitionsWithIndex((partitionIdx, it) => {
        if (partitionIdx > 0) {
          for {
            (elem, idx) <- it.zipWithIndex
          } yield
            (partitionIdx - 1, idx) -> elem
        } else
          Iterator()
      })
      .repartitionAndSortWithinPartitions(KeyPartitioner(rdd))
      .values
}

object BorrowElemsRDD {
  implicit def rddToBorrowElemsRDD[T: ClassTag](rdd: RDD[T]): BorrowElemsRDD[T] = new BorrowElemsRDD(rdd)
}
