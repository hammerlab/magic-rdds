package org.hammerlab.bytes

import caseapp.core.ArgParser

/**
 * Wrapper for representation of a number of bytes
 */
sealed abstract class Bytes(scale: Long) {
  def bytes: Long = value * scale
  def value: Int

  override def toString: String =
    s"$value${getClass.getSimpleName}"
}

case class  B(value: Int) extends Bytes(1L <<  0)
case class KB(value: Int) extends Bytes(1L << 10)
case class MB(value: Int) extends Bytes(1L << 20)
case class GB(value: Int) extends Bytes(1L << 30)
case class TB(value: Int) extends Bytes(1L << 40)
case class PB(value: Int) extends Bytes(1L << 50)
case class EB(value: Int) extends Bytes(1L << 60)

object Bytes {

  val bytesStrRegex = """^(\d+)([KMGTPE]?)B?$""".r

  def apply(bytesStr: String): Bytes = {
    bytesStr.toUpperCase() match {
      case bytesStrRegex(numStr, suffix) ⇒
        val num = numStr.toInt
        Option(suffix) match {
          case Some("K") ⇒ KB(num)
          case Some("M") ⇒ MB(num)
          case Some("G") ⇒ GB(num)
          case Some("T") ⇒
            if (num < (8 << 20))
              TB(num)
            else
              throw BytesOverflowException(bytesStr)
          case Some("P") ⇒
            if (num < (8 << 10))
              PB(num)
            else
              throw BytesOverflowException(bytesStr)
          case Some("E") ⇒
            if (num < 8)
              EB(num)
            else
              throw BytesOverflowException(bytesStr)
          case Some("") | None ⇒ B(num)
          case _ ⇒
            // can't happen, just here to make compiler not warn
            throw new Exception(s"bug in Bytes regex parsing…")
        }
      case _ ⇒
        throw BadBytesString(bytesStr)
    }
  }

  implicit def unwrapBytes(bytes: Bytes): Long = bytes.bytes

  implicit val bytesParser =
    ArgParser.instance[Bytes]("bytes") {
      bytes ⇒
        Right(
          Bytes(
            bytes
          )
        )
    }
}

case class BadBytesString(str: String)
  extends IllegalArgumentException(str)

case class BytesOverflowException(str: String)
  extends IllegalArgumentException(str)
