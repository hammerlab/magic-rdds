package org.apache.spark.sortwith

import com.holdenkarau.spark.testing.SharedSparkContext
import SortWithRDD._
import org.scalatest.{FunSuite, Matchers}

import scala.reflect.ClassTag

case class Foo(n: Int, s: String)

class SortWithRDDTest extends FunSuite with SharedSparkContext with Matchers {
  import CmpFns._

  def testArr[T: ClassTag](name: String, arr: Seq[T], cmpFn: (T, T) => Int) = {
    test(name) {
      val expected = arr.sortWith(boolify(cmpFn))
      sc.parallelize(arr).sortWith(cmpFn).collect() should be(expected)
      sc.parallelize(arr).sortWith(cmpFn, ascending = false).collect() should be(expected.reverse)
    }
  }

  testArr("test already sorted", 1 to 1000, Ordering.Int.compare)

  val l1 = List(84, 96, 89, 17, 37, 88, 14, 56, 59, 99, 54, 43, 72, 2, 10, 80, 73, 9, 15, 20, 86, 62, 76, 16, 29, 32, 66, 97, 22, 65, 24, 31, 19, 53, 42, 64, 39, 35, 85, 98, 95, 49, 87, 50, 57, 30, 45, 21, 40, 91, 61, 34, 63, 83, 51, 13, 12, 75, 3, 92, 11, 28, 82, 7, 44, 70, 18, 77, 4, 55, 60, 100, 5, 69, 23, 79, 71, 67, 48, 81, 25, 38, 27, 93, 8, 6, 74, 46, 52, 33, 1, 94, 90, 36, 78, 26, 58, 47, 41, 68)
  testArr("random1", l1, Ordering.Int.compare)

  val words = List("Acipenseres", "octave", "diffusiometer", "intersomnial", "wrestling", "grittle", "reassortment", "diluvia", "Albainn", "contemplable", "catawamptiously", "rampantly", "bayardly", "moonshine", "wourari", "dueler", "pseudoholoptic", "Mameluke", "atocha", "cyanocarbonic", "sudder", "giantess", "Quiina", "rufus", "Tyndallize", "unprofessional", "Ione", "pollute", "joiningly", "fadedness", "stuntiness", "zoisitization", "Patrice", "regraduate", "desuete", "paleometallic", "profectional", "knapsacked", "preramus", "pikelet", "Bdellostomidae", "unheavenly", "prerestoration", "monarchize", "koila", "subtranslucent", "unhonoured", "merocyte", "gravely", "Adamite", "idiasm", "semivocalic", "Limnobium", "exocentric", "lysigenic", "mediation", "height", "pontooning", "clearly", "schistocormus", "basidiosporous", "Tarai", "Khartoumer", "apneumatic", "misapplier", "Rhynchops", "weakheartedly", "waxwork", "rusk", "parazonium", "mangosteen", "puddinghead", "scutular", "beroll", "flavoprotein", "launchful", "whitebill", "wearily", "baith", "commando", "moleskin", "alangin", "polytheistic", "Balaena", "nettly", "wife", "corticifugally", "jinjili", "summercastle", "dianisidine", "itinerancy", "aloed", "merosthenic", "impertinence", "sialagogue", "sudsy", "parhelion", "underbrim", "deadlily", "Dakota")
  val ints = List(216902, 35006, 157230, 141699, 104192, 228050, 94338, 137340, 205171, 226828, 191152, 26917, 18514, 229879, 114260, 124826, 169602, 27841, 228542, 204506, 123478, 22032, 14667, 201903, 46867, 30970, 30734, 34954, 204276, 130985, 69400, 60888, 166362, 204149, 227037, 88896, 98264, 112945, 127739, 159540, 163496, 6160, 89863, 216773, 105163, 130594, 107449, 15398, 190176, 46842, 47672, 184698, 166400, 216022, 152304, 159001, 44894, 53718, 174122, 125119, 106445, 232906, 176761, 65903, 216553, 196557, 134379, 177325, 43119, 23299, 229065, 112225, 122387, 177990, 75282, 233018, 125550, 158711, 17493, 217319, 54401, 85059, 80703, 13783, 86759, 204195, 54885, 107576, 39571, 74276, 5298, 162207, 113867, 204398, 127554, 35810, 108595, 106200, 70756, 52665)
  val foos = for { (i, w) <- ints.zip(words) } yield { Foo(i, w) }

  testArr("foo ints 1", foos.slice(0, 1), intCmpFn)
  testArr("foo ints 3", foos.slice(0, 3), intCmpFn)
  testArr("foo ints 10", foos.slice(0, 10), intCmpFn)
  testArr("foo ints 20", foos.slice(0, 20), intCmpFn)
  testArr("foo ints 100", foos, intCmpFn)

  testArr("foo strs 1", foos.slice(0, 1), strCmpFn)
  testArr("foo strs 3", foos.slice(0, 3), strCmpFn)
  testArr("foo strs 10", foos.slice(0, 10), strCmpFn)
  testArr("foo strs 20", foos.slice(0, 20), strCmpFn)
  testArr("foo strs 100", foos, strCmpFn)

  testArr("foo vowels 1", foos.slice(0, 1), vowelCmpFn)
  testArr("foo vowels 3", foos.slice(0, 3), vowelCmpFn)
  testArr("foo vowels 10", foos.slice(0, 10), vowelCmpFn)
  testArr("foo vowels 20", foos.slice(0, 20), vowelCmpFn)
  testArr("foo vowels 100", foos, vowelCmpFn)

}

object CmpFns extends Serializable {

  def boolify[T: ClassTag](cmpFn: (T, T) => Int): (T, T) => Boolean = {
    (t1, t2) => cmpFn(t1, t2) < 0
  }

  def intCmpFn(o1: Foo, o2: Foo): Int = {
    implicitly[Ordering[Int]].compare(o1.n, o2.n)
  }

  def strCmpFn(o1: Foo, o2: Foo): Int = {
    implicitly[Ordering[String]].compare(o1.s, o2.s)
  }

  def isVowel(c: Char) = c.toLower match {
    case 'a'|'e'|'i'|'o'|'u' => true
    case _ => false
  }

  def vowelCmpFn(o1: Foo, o2: Foo): Int = {
    val (s1, s2) = (o1.s, o2.s)
    (isVowel(s1(0)), isVowel(s2(0))) match {
      case (true, true) => intCmpFn(o1, o2)
      case (false, false) => strCmpFn(o1, o2)
      case (true, false) => -1
      case (false, true) => 1
    }
  }

}
