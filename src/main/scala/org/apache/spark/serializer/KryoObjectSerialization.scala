package org.apache.spark.serializer

import java.io.{EOFException, IOException, InputStream, OutputStream}

import com.esotericsoftware.kryo.io.{Input => KryoInput, Output => KryoOutput}
import com.esotericsoftware.kryo.{Kryo, KryoException}
import org.apache.spark.util.NextIterator

import scala.reflect.ClassTag

class PublicKryoSerializerInstance(ks: KryoSerializer) extends KryoSerializerInstance(ks)

class KryoObjectSerializationStream(serInstance: KryoSerializerInstance,
                                    outStream: OutputStream,
                                    writeClass: Boolean = false) extends SerializationStream {

  private[this] var output: KryoOutput = new KryoOutput(outStream)
  private[this] var kryo: Kryo = serInstance.borrowKryo()

  override def writeObject[T: ClassTag](t: T): SerializationStream = {
    if (writeClass)
      kryo.writeClassAndObject(output, t)
    else
      kryo.writeObject(output, t)

    this
  }

  override def flush() {
    if (output == null) {
      throw new IOException("Stream is closed")
    }
    output.flush()
  }

  override def close() {
    if (output != null) {
      try {
        output.close()
      } finally {
        serInstance.releaseKryo(kryo)
        kryo = null
        output = null
      }
    }
  }
}

class KryoObjectDeserializationStream(serInstance: KryoSerializerInstance,
                                      inStream: InputStream,
                                      readClass: Boolean = false) {

  private[this] var input: KryoInput = new KryoInput(inStream)
  private[this] var kryo: Kryo = serInstance.borrowKryo()
  private[this] var finalBytesRead: Long = 0

  /**
    * Read the elements of this stream through an iterator. This can only be called once, as
    * reading each element will consume data from the input source.
    */
  def asIterator[T: ClassTag]: Iterator[T] = new NextIterator[T] {
    override protected def getNext() = {
      try {
        readObject[T]()
      } catch {
        case eof: EOFException =>
          finished = true
          null.asInstanceOf[T]
      }
    }

    override protected def close() {
      KryoObjectDeserializationStream.this.close()
    }
  }

  def bytesRead(): Long = if (input == null) finalBytesRead else input.total()

  def read[T]()(implicit ct: ClassTag[T]): T = {
    if (readClass)
      kryo.readClassAndObject(input).asInstanceOf[T]
    else
      kryo.readObject(input, ct.runtimeClass).asInstanceOf[T]
  }

  def readObject[T: ClassTag](): T = {
    try {
      read()
    } catch {
      // DeserializationStream uses the EOF exception to indicate stopping condition.
      case e: KryoException if e.getMessage.toLowerCase.contains("buffer underflow") =>
        throw new EOFException
    }
  }

  def close() {
    if (input != null) {
      try {
        // Kryo's Input automatically closes the input stream it is using.
        input.close()
      } finally {
        serInstance.releaseKryo(kryo)
        kryo = null
        finalBytesRead = input.total()
        input = null
      }
    }
  }
}
