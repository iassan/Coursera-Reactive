package patmat

import org.scalatest.FunSuite

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import patmat.Huffman._

@RunWith(classOf[JUnitRunner])
class HuffmanSuite extends FunSuite {
  trait TestTrees {
    val t1 = Fork(Leaf('a',2), Leaf('b',3), List('a','b'), 5)
    val t2 = Fork(Fork(Leaf('a',2), Leaf('b',3), List('a','b'), 5), Leaf('d',4), List('a','b','d'), 9)
  }

  test("weight of a larger tree") {
    new TestTrees {
      assert(weight(t1) === 5)
    }
  }

  test("chars of a larger tree") {
    new TestTrees {
      assert(chars(t2) === List('a','b','d'))
    }
  }

  test("string2chars(\"hello, world\")") {
    assert(string2Chars("hello, world") === List('h', 'e', 'l', 'l', 'o', ',', ' ', 'w', 'o', 'r', 'l', 'd'))
  }

	test("times") {
		assert(times(string2Chars("babajaga")) === List(('j', 1), ('g', 1), ('b', 2), ('a', 4)))
	}

	test("makeOrderedLeafList for some frequency table") {
    assert(makeOrderedLeafList(List(('t', 2), ('e', 1), ('x', 3))) === List(Leaf('e',1), Leaf('t',2), Leaf('x',3)))
  }

  test("combine of some leaf list") {
    val leaflist = List(Leaf('e', 1), Leaf('t', 2), Leaf('x', 4))
    assert(combine(leaflist) === List(Fork(Leaf('e',1),Leaf('t',2),List('e', 't'),3), Leaf('x',4)))
  }

	test("decode my message") {
		val codeTree = createCodeTree(string2Chars("baba jaga"))
		//println("codeTree: " + codeTree)
		val code: List[Bit] = List(1,0,0,1,0,0,1,1,0,1,1,1,0,0,1,1,1,1,0)
		assert(decode(codeTree, code) === List('b','a','b','a',' ','j','a','g','a'))
	}

	test("decode secret message") {
		assert(decodedSecret === List('h', 'u', 'f', 'f', 'm', 'a', 'n', 'e', 's', 't', 'c', 'o', 'o', 'l'))
	}

	test("encode \"baba jaga\"") {
		val codeTree = createCodeTree(string2Chars("baba jaga"))
		val message = List('b','a','b','a',' ','j','a','g','a')
		assert(encode(codeTree)(message) === List(1,0,0,1,0,0,1,1,0,1,1,1,0,0,1,1,1,1,0))
	}

  test("decode and encode a very short text should be identity") {
    new TestTrees {
      assert(decode(t1, encode(t1)("ab".toList)) === "ab".toList)
    }
  }
}
