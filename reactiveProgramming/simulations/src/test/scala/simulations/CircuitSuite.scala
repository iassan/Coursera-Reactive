package simulations

import org.scalatest.FunSuite

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CircuitSuite extends CircuitSimulator with FunSuite {
  val InverterDelay = 1
  val AndGateDelay = 3
  val OrGateDelay = 5

  test("andGate example") {
    val in1, in2, out = new Wire
    andGate(in1, in2, out)
    in1.setSignal(false)
    in2.setSignal(false)
    run

    assert(out.getSignal === false, "and 1")

    in1.setSignal(true)
    run

    assert(out.getSignal === false, "and 2")

    in2.setSignal(true)
    run

    assert(out.getSignal === true, "and 3")
  }

  test("orGate2") {
    val in1, in2, out = new Wire
    orGate2(in1, in2, out)
    in1.setSignal(false)
    in2.setSignal(false)
    run

    assert(out.getSignal === false)

    in1.setSignal(true)
    run

    assert(out.getSignal === true)

    in2.setSignal(true)
    run

    assert(out.getSignal === true)

    in1.setSignal(false)
    run

    assert(out.getSignal === true)
  }

  test("demux 1") {
    val in, out = new Wire
    demux(in, Nil, List(out))
    println(s"test1, in: $in, out: $out")
    in.setSignal(true)
    run

    assert(out.getSignal === true)

    in.setSignal(false)
    run

    assert(out.getSignal === false)
  }

  test("demux 2") {
    val in, c, out1, out2 = new Wire
    println(s"test2, in: $in, c: $c, out1: $out1, out2: $out2")
    demux(in, List(c), List(out2, out1))
    c.setSignal(true)
    in.setSignal(true)
    run

    assert(out1.getSignal === false)
    assert(out2.getSignal === true)
  }

  //
  // to complete with tests for orGate, demux, ...
  //

}
