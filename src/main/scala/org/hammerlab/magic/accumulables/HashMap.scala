package org.hammerlab.magic.accumulables

import spire.math.Numeric

import scala.collection.mutable

case class HashMap[T, N: Numeric](map: mutable.HashMap[T, N]) {
  override def toString: String = "…"
}

object HashMap {
  implicit def mapToHashMap[T, N: Numeric](map: mutable.HashMap[T, N]): HashMap[T, N] = new HashMap(map)
  implicit def hashMapToMap[T, N: Numeric](hashMap: HashMap[T, N]): mutable.HashMap[T, N] = hashMap.map

  def apply[T, N: Numeric](): HashMap[T, N] = new HashMap(mutable.HashMap.empty[T, N])
}
