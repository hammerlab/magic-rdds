package org.apache.spark.sortwith

import java.util.{Arrays, Comparator}

import scala.reflect.ClassTag

object SortUtils {
  def makeBinarySearch[T : ClassTag](cmpFn: (T, T) => Int) : (Array[T], T) => Int = {
    val comparator = new SerializableComparator[T](cmpFn)
    (l, x) => Arrays.binarySearch(l.asInstanceOf[Array[AnyRef]], x, comparator.asInstanceOf[Comparator[Any]])
  }
}

class SerializableComparator[T: ClassTag](cmpFn: (T, T) => Int)
  extends Comparator[T]
    with Serializable {
  override def compare(o1: T, o2: T): Int = cmpFn(o1, o2)
}
