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
  // key -> (id, where to send confirmation about operation success/failure (can be null), is persisted (are we NOT waiting for persistence), set of replicators, from which we're still waiting for a reply)
  var acks = Map.empty[String, (Long, ActorRef, Boolean, Set[ActorRef])]

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

  /* TODO Behavior for the leader role. */
  def leader: Receive = {
    case Insert(key, value, id) =>
      kv = kv.updated(key, value)
      initAck(key, id, sender, waitForPersistence = true)
      val insert = Insert(key, value, id)
      replicateOperation(insert)
      var queue = Queue.empty[(Persist, ActorRef)]
      val persist = new Persist(key, Some(value), id)
      persistence ! persist
      queue = queue :+ (persist, sender)
      context.setReceiveTimeout(persistTimeout)
      acks = acks.updated(key, (id, sender, false, replicators))
      context.become(leaderWaitingForAck(queue), discardOld = true)
    case Remove(key, id) =>
      kv = kv - key
      initAck(key, id, sender, waitForPersistence = true)
      val remove = Remove(key, id)
      replicateOperation(remove)
      // replicate
      var queue = Queue.empty[(Persist, ActorRef)]
      val persist = new Persist(key, None, id)
      persistence ! persist
      queue = queue :+ (persist, sender)
      context.setReceiveTimeout(persistTimeout)
      acks = acks.updated(key, (id, sender, false, replicators))
      context.become(leaderWaitingForAck(queue), discardOld = true)
    case Get(key, id) =>
      sender ! new GetResult(key, kv.get(key), id)
    case Replicated(key, id) =>
      acks = acks.updated(key, (acks(key)._1, acks(key)._2, acks(key)._3, acks(key)._4 - sender))
      sendAckIfPossible(key)
    //case Replicas(replicas) =>

  }

  def leaderWaitingForAck(persistRequestQueue: Queue[(Persist, ActorRef)]): Receive = {
    case Insert(key, value, id) =>
      kv = kv.updated(key, value)
      initAck(key, id, sender, waitForPersistence = true)
      val insert = Insert(key, value, id)
      replicateOperation(insert)
      val persist = new Persist(key, Some(value), id)
      val queue = persistRequestQueue :+ (persist, sender)
      context.become(leaderWaitingForAck(queue))
    case Remove(key, id) =>
      kv = kv - key
      initAck(key, id, sender, waitForPersistence = true)
      val remove = Remove(key, id)
      replicateOperation(remove)
      val persist = new Persist(key, None, id)
      val queue = persistRequestQueue :+ (persist, sender)
      context.become(leaderWaitingForAck(queue))
    case Get(key, id) =>
      sender ! new GetResult(key, kv.get(key), id)
    case Persisted(key, id) =>
      acks = acks.updated(key, (acks(key)._1, acks(key)._2, true, acks(key)._4))
      sendAckIfPossible(key)
      context.setReceiveTimeout(Duration.Undefined)
      if (persistRequestQueue.tail.isEmpty) {
        context.become(leader, discardOld = true)
      } else {
        val next = persistRequestQueue.tail.head
        persistence ! next._1
        context.setReceiveTimeout(persistTimeout)
        context.become(leaderWaitingForAck(persistRequestQueue.tail), discardOld = true)
      }
    case Replicated(key, id) =>
      acks = acks.updated(key, (acks(key)._1, acks(key)._2, acks(key)._3, acks(key)._4 - sender))
      sendAckIfPossible(key)
    case ReceiveTimeout =>
      context.setReceiveTimeout(persistTimeout)
      persistence ! persistRequestQueue.head._1
    //case Replicas(replicas) =>

  }

  private def sendAckIfPossible(key: String) = {
    if (acks(key)._3 && acks(key)._4.isEmpty) {
      if (acks(key)._2 != null)
        acks(key)._2 ! OperationAck(acks(key)._1)
      acks = acks - key
    }
  }

  private def replicateOperation(operation: Operation) = {
    operation match {
      case Insert(key, value, id) =>
        for {
          replicator <- replicators
        } yield {
          replicator ! Replicate(key, Some(value), id)
        }
      case Remove(key, id) =>
        for {
          replicator <- replicators
        } yield {
          replicator ! Replicate(key, None, id)
        }
    }
  }

  private def initAck(key: String, id: Long, sendAckTo: ActorRef, waitForPersistence: Boolean) = {
    acks = acks.updated(key, (id, sendAckTo, !waitForPersistence, replicators))
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
