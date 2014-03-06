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

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  //var acks = Map.empty[Long, (ActorRef, Replicate)]
  // seq -> (key, id)
  var acks = Map.empty[Long, (String, Long)]
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
      //acks = acks.updated(snapshot.seq, (null, new Replicate(key, valueOption, id)))
      acks = acks.updated(snapshot.seq, (key, id))
      replica ! snapshot
      context.setReceiveTimeout(receiveTimeout)
      context.become(waitingForAck(snapshot), discardOld = false)
    case SnapshotAck(key, seq) =>
      log.warning(s"Got SnapshotAck when not expecting one, key: $key, seq: $seq")
    case ReceiveTimeout =>
      // not waiting for anything
      context.setReceiveTimeout(Duration.Undefined)
  }

  def waitingForAck(snapshot: Snapshot): Receive = {
    case Replicate(key, valueOption, id) =>
      val snapshot = new Snapshot(key, valueOption, nextSeq)
      pending = pending :+ snapshot
      acks = acks.updated(snapshot.seq, (key, id))
    case SnapshotAck(key, seq) =>
      //acks = acks.updated(seq, (sender, acks(seq)._2))
      if (acks.contains(seq)) {
        context.parent ! new Replicated(key, acks(seq)._2)
        acks = acks - seq
        if (pending.nonEmpty) {
          val nextSnapshot = pending.head
          pending = pending.tail
          //acks = acks.updated(nextSnapshot.seq, (null, new Replicate(key, nextSnapshot.valueOption, acks(nextSnapshot.seq)._2.id)))
          replica ! nextSnapshot
          context.setReceiveTimeout(receiveTimeout)
          context.become(waitingForAck(nextSnapshot))
        } else {
          context.setReceiveTimeout(Duration.Undefined)
          context.unbecome()
        }
      } else {
        log.warning(s"Got SnapshotAck($key, $seq) which was not expected")
      }
    case ReceiveTimeout =>
      replica ! snapshot
  }
}
