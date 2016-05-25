package org.hammerlab.magic.rdd

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class BorrowElemsRDD[T: ClassTag](@transient rdd: RDD[T]) extends Serializable {

  def shift(n: Int, fill: T): RDD[T] = shift(n, Some(fill))
  def shift(n: Int, fillOpt: Option[T] = None): RDD[T] = {
    copyN(
      n,
      (partitionIdx: Int, it: Iterator[T], arr: Array[T]) =>
        (
          if (partitionIdx == 0)
            it
          else
            it.drop(n)
        ) ++ arr.toIterator,
      fillOpt
    )
  }

  def copy(n: Int, fill: T): RDD[T] = copy(n, Some(fill))
  def copy(n: Int, fillOpt: Option[T] = None): RDD[T] = {
    copyN(
      n,
      (_: Int, it: Iterator[T], arr: Array[T]) => it ++ arr.toIterator,
      fillOpt
    )
  }

  def copyN(n: Int, fn: (Int, Iterator[T], Array[T]) => Iterator[T], fill: T): RDD[T] = copyN(n, fn, Some(fill))
  def copyN(n: Int, fn: (Int, Iterator[T], Array[T]) => Iterator[T], fillOpt: Option[T] = None): RDD[T] = {
    val N = rdd.getNumPartitions

    val firstSplit: RDD[(Int, Array[T])] =
      rdd.mapPartitionsWithIndex((idx, iter) =>
        if (idx == 0)
          fillOpt.map(fill => (N - 1) -> Array.fill(n)(fill)).toIterator
        else {
          val copiedElems = iter.take(n).toArray
          if (copiedElems.length < n) {
            throw new NoSuchElementException(
              s"Found only ${copiedElems.length} elements in partition $idx; needed ≥ $n"
            )
          }
          Iterator(
            (idx - 1) → copiedElems
          )
        }
      ).partitionBy(new KeyPartitioner(N))

    rdd.zipPartitions(firstSplit)((iter, tailIter) ⇒
      if (tailIter.hasNext) {
        val (partitionIdx, arr) = tailIter.next()
        fn(partitionIdx, iter, arr)
      } else
        fn(N - 1, iter, Array())
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
