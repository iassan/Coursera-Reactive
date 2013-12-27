package kvstore

import akka.actor._
import scala.concurrent.duration._

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)

  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
  val receiveTimeout = 100.milliseconds
}

class Replicator(val replica: ActorRef) extends Actor with ActorLogging {
  import Replicator._
  import Replica._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]

  var _seqCounter = 0L
  def nextSeq = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  /* TODO Behavior for the Replicator. */
  def receive = waitingForTask

  val waitingForTask: Receive = {
    case Replicate(key, valueOption, id) =>
      val snapshot = new Snapshot(key, valueOption, nextSeq)
      //pending = pending :+ snapshot
      acks = acks.updated(snapshot.seq, (null, new Replicate(key, valueOption, id)))
      replica ! snapshot
      context.setReceiveTimeout(receiveTimeout)
      context.become(waitingForAck(snapshot), discardOld = true)
    case SnapshotAck(key, seq) =>
      log.warning(s"Got SnapshotAck when not expecting one, key: $key, seq: $seq")
    case ReceiveTimeout =>
      // not waiting for anything
      context.setReceiveTimeout(Duration.Undefined)
  }

  def waitingForAck(snapshot: Snapshot): Receive = {
    case Replicate(key, valueOption, id) =>
      pending = pending :+ new Snapshot(key, valueOption, nextSeq)
    case SnapshotAck(key, seq) =>
      acks = acks.updated(seq, (sender, acks(seq)._2))
      context.parent ! new Replicated(key, acks(seq)._2.id)
      if (pending.nonEmpty) {
        val nextSnapshot = pending.head
        pending = pending.tail
        acks = acks.updated(nextSnapshot.seq, (null, new Replicate(key, nextSnapshot.valueOption, acks(nextSnapshot.seq)._2.id)))
        replica ! nextSnapshot
        context.setReceiveTimeout(receiveTimeout)
        context.become(waitingForAck(nextSnapshot), discardOld = true)
      } else {
        context.setReceiveTimeout(Duration.Undefined)
        context.become(waitingForTask, discardOld = true)
      }
    case ReceiveTimeout =>
      replica ! snapshot
  }

}
