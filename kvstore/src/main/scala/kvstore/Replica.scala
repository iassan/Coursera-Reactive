package kvstore

import akka.actor._
import kvstore.Arbiter._
import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart
import scala.annotation.tailrec
import akka.pattern.{ ask, pipe }
import scala.concurrent.duration._
import akka.util.Timeout
import scala.concurrent.duration._
import scala.Some
import akka.actor.OneForOneStrategy

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor with ActorLogging {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  var expectedSeq = 0L
  override val supervisorStrategy = OneForOneStrategy() {
    case _: Exception => Restart
  }
  val persistence = context.actorOf(persistenceProps)
  val persistTimeout = 100.milliseconds

  override def preStart() {
    arbiter ! Join
  }

  def receive = {
    case JoinedPrimary => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case Insert(key, value, id) =>
      kv = kv.updated(key, value)
      // replicate
      // persist
      sender ! new OperationAck(id)
    case Remove(key, id) =>
      kv = kv - key
      // replicate
      // persist
      sender ! new OperationAck(id)
    case Get(key, id) =>
      sender ! new GetResult(key, kv.get(key), id)
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case Get(key, id) =>
      sender ! new GetResult(key, kv.get(key), id)
    case Snapshot(key, valueOption, seq) =>
      if (seq < expectedSeq)
        sender ! SnapshotAck(key, seq)
      if (seq == expectedSeq) {
        valueOption match {
          case None => kv = kv - key
          case Some(value) => kv = kv.updated(key, value)
        }
        val persist = new Persist(key, valueOption, seq)
        persistence ! persist
        context.setReceiveTimeout(persistTimeout)
        context.become(replicaWaitingForPersistence(persist, sender))
      }
  }

  def replicaWaitingForPersistence(persist: Persist, replicator: ActorRef): Receive = {
    case Get(key, id) =>
      sender ! new GetResult(key, kv.get(key), id)
    case Persisted(key, id) =>
      if (id == persist.id) {
        context.setReceiveTimeout(Duration.Undefined)
        replicator ! SnapshotAck(key, id)
        expectedSeq = id + 1
        context.become(replica, discardOld = true)
      } else {
        log.warning(s"Got Persisted message for different (key, id), got ($key, $id), expected (?, ${persist.id}})")
      }
    case ReceiveTimeout =>
      context.setReceiveTimeout(persistTimeout)
      persistence ! persist
  }
}
