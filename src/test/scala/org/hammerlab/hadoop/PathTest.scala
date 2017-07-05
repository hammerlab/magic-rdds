package org.hammerlab.hadoop

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream }

import org.hammerlab.test.Suite
import org.hammerlab.test.resources.File

class PathTest
  extends Suite {

  test("serde") {
    implicit val conf = Configuration()
    val path = Path(File("log4j.properties").uri)

    val baos = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(baos)

    out.writeObject(path)

    out.close()

    val bytes = baos.toByteArray

    bytes.length should be(68833)

    val bais = new ByteArrayInputStream(bytes)
    val in = new ObjectInputStream(bais)

    val path2 = in.readObject().asInstanceOf[Path]

    path2 should be(path)
  }

}
