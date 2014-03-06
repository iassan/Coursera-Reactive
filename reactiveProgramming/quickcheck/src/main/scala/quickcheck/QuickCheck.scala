package quickcheck

import org.scalacheck._
import Arbitrary._
import Gen._
import Prop._

abstract class QuickCheckHeap extends Properties("Heap") with IntHeap {

  property("min1") = forAll {
    a: A =>
      val h = insert(a, empty)
      findMin(h) == a
  }

  property("min of two") = forAll {
    (a: A, b: A) =>
      val h = insert(b, insert(a, empty))
      findMin(h) == (if (a < b) a else b)
  }

  property("empty after delete last") = forAll {
    a: A =>
      val h = insert(a, empty)
      isEmpty(deleteMin(h))
  }

  property("min of melded heaps needs to be a minimum of mins for source heaps") = forAll {
    (h1: H, h2: H) =>
      def min(x: A, y: A) = if (x < y) x else y
      val h = meld(h1, h2)
      findMin(h) == min(findMin(h1), findMin(h2))
  }

  def checkNextElement(h: H, lastElem: A): Boolean = {
    if (isEmpty(h))
      true
    else {
      if (ord.lt(findMin(h), lastElem))
        false
      else
        checkNextElement(deleteMin(h), findMin(h))
    }
  }

  property("getting all element from heap should be an ordered sequence") = forAll {
    h: H =>
      if (isEmpty(h))
        true
      else
        checkNextElement(deleteMin(h), findMin(h))
  }

  property("melding two heaps should provide a proper heap") = forAll {
    (h1: H, h2: H) =>
      val h = meld(h1, h2)
      if (isEmpty(h))
        true
      else
        checkNextElement(deleteMin(h), findMin(h))
  }

  property("findMin should return previously added minimal value") = forAll {
    h: H =>
      if (!isEmpty(h)) {
        val m = findMin(h)
        if (m != Int.MinValue) {
          val newH = insert(m - 1, h)
          findMin(newH) == (m - 1)
        } else {
          true
        }
      } else {
        true
      }
  }

  property("removing and adding min element should not change the behaviour") = forAll {
    h: H =>
      if (!isEmpty(h)) {
        val minEl = findMin(h)
        val newH = insert(minEl, deleteMin(h))
        findMin(newH) == minEl
      } else {
        true
      }
  }

  property("deleted element should not be on heap") = forAll {
    (a: A, b: A) =>
      if (a != b) {
        val h = insert(b, insert(a, empty))
        findMin(h) != findMin(deleteMin(h))
      } else {
        true
      }
  }

  property("element added twice, should be there twice 1") = forAll {
    a: A =>
      val h = insert(a, insert(a, empty))
      findMin(h) == a
  }

  property("element added twice, should be there twice 2") = forAll {
    a: A =>
      val h = insert(a, insert(a, empty))
      findMin(deleteMin(h)) == a
  }

  property("element added twice, should be there twice 1") = forAll {
    a: A =>
      val h = insert(a, insert(a, empty))
      !isEmpty(deleteMin(h))
  }

  property("after deleting min element next findMin should indeed return nex minimal element") = forAll {
    (a: A, b: A, c: A) =>
      val l = List(a, b, c).sorted
      val h = insert(a, insert(b, insert(c, empty)))
      findMin(deleteMin(h)) == l(1)
  }

  lazy val genHeap: Gen[H] = for {
    x <- arbitrary[Int]
    h <- oneOf(value(empty), genHeap)
  } yield insert(x, h)

  implicit lazy val arbHeap: Arbitrary[H] = Arbitrary(genHeap)

}
