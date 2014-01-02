package kvstore

import akka.actor._
import kvstore.Arbiter._
import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart
import scala.concurrent.duration._
import scala.Some
import akka.actor.OneForOneStrategy
import java.util.Date

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

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]
  // key -> (id, where to send confirmation about operation success/failure (can be null), is persisted (are we NOT waiting for persistence), set of replicators, from which we're still waiting for a reply, when timeout is due)
  var acks = Map.empty[String, (Long, ActorRef, Boolean, Set[ActorRef], Long)]

  var operations = Queue.empty[Operation]

  var expectedSeq = 0L
  override val supervisorStrategy = OneForOneStrategy() {
    case _: Exception => Restart
  }
  val persistence = context.actorOf(PersistenceProxy.props(persistenceProps))
  val persistTimeout = 100.milliseconds

  override def preStart() {
    arbiter ! Join
  }

  def receive = {
    case JoinedPrimary =>
      context.setReceiveTimeout(persistTimeout)
      context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /* TODO Behavior for the leader role. */
  def leader: Receive = {
    case Insert(key, value, id) =>
      kv = kv.updated(key, value)
      checkForTimeouts
      initAck(key, id, sender, waitForPersistence = true)
      replicateOperation(Insert(key, value, id))
      persistence ! Persist(key, Some(value), id)
    case Remove(key, id) =>
      kv = kv - key
      checkForTimeouts
      initAck(key, id, sender, waitForPersistence = true)
      replicateOperation(Remove(key, id))
      persistence ! Persist(key, None, id)
    case Get(key, id) =>
      sender ! new GetResult(key, kv.get(key), id)
      checkForTimeouts
    case Replicated(key, id) =>
      if (acks.contains(key)) {
        acks = acks.updated(key, (acks(key)._1, acks(key)._2, acks(key)._3, acks(key)._4 - sender, acks(key)._5))
        sendAckIfPossible(key)
      }
      checkForTimeouts
    case Persisted(key, id) =>
      if (acks.contains(key)) {
        acks = acks.updated(key, (acks(key)._1, acks(key)._2, true, acks(key)._4, acks(key)._5))
        sendAckIfPossible(key)
      }
      checkForTimeouts
    case Replicas(replicas) =>
      processNewReplicasSet(replicas)
      checkForTimeouts
    case ReceiveTimeout =>
      checkForTimeouts
  }

  private def sendAckIfPossible(key: String) = {
    if (acks.contains(key) && acks(key)._3 && acks(key)._4.isEmpty) {
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
      case Get(_, _) =>
    }
    operations = operations :+ operation
  }

  private def initAck(key: String, id: Long, sendAckTo: ActorRef, waitForPersistence: Boolean) = {
    acks = acks.updated(key, (id, sendAckTo, !waitForPersistence, replicators, new Date().getTime + 1000))
  }

  private def checkForTimeouts = {
    val now = new Date().getTime
    var keysToRemove = Set.empty[String]
    for {
      (k, v) <- acks
      if v._5 < now
    } yield {
      keysToRemove = keysToRemove + k
      v._2 ! OperationFailed(v._1)
    }
    for {
      k <- keysToRemove
    } yield {
      acks = acks - k
    }
  }

  private def processNewReplicasSet(replicas: Set[ActorRef]) = {
    val replicasToDrop = secondaries.keySet -- replicas
//    log.info(s"replicasToDrop: $replicasToDrop")
    for {
      key <- acks.keySet
    } yield {
      val v = acks(key)
      acks = acks.updated(key, (v._1, v._2, v._3, v._4 -- replicasToDrop.map(secondaries(_)), v._5))
//      log.info(s"removing waiting for acks from removed replicas, key: $key, persistence: ${v._3}, replicas: ${v._4.size}")
//      for {
//        replica <- v._4
//      } yield {
//        log.info(s"outstanding replica: $replica")
//      }
      sendAckIfPossible(key)
    }
    for {
      replica <- replicasToDrop
    } yield {
      replicators = replicators - secondaries(replica)
      secondaries(replica) ! PoisonPill
      secondaries - replica
    }

    val newReplicas = replicas - self -- secondaries.keySet
    for {
      replica <- newReplicas
    } yield {
      val replicator = context.actorOf(Replicator.props(replica))
      replicators = replicators + replicator
      secondaries = secondaries.updated(replica, replicator)
//      log.info(s"Adding new replica: $replica, replicator: $replicator")
      for {
        operation <- operations
      } yield {
//        log.info(s"Replaying operation $operation for replica $replica, replicator: $replicator")
        operation match {
          case Insert(key, value, id) =>
            replicator ! Replicate(key, Some(value), id)
            if (acks.contains(key) && acks(key)._1 == id) {
              val v = acks(key)
              acks = acks.updated(key, (v._1, v._2, v._3, v._4 + replicator, v._5))
            }
          case Remove(key, id) =>
            replicator ! Replicate(key, None, id)
            if (acks.contains(key) && acks(key)._1 == id) {
              val v = acks(key)
              acks = acks.updated(key, (v._1, v._2, v._3, v._4 + replicator, v._5))
            }
          case Get(key, id) =>
            log.error(s"Got Get($key, $id) operation to replicate...")
        }
      }
    }
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
        //context.setReceiveTimeout(persistTimeout)
        context.become(replicaWaitingForPersistence(Queue(persist), sender), discardOld = false)
      }
  }

  def replicaWaitingForPersistence(persistQueue: Queue[Persist], replicator: ActorRef): Receive = {
    case Get(key, id) =>
      sender ! new GetResult(key, kv.get(key), id)
    case Persisted(key, id) =>
      // wysyłamy potwierdzenie z powrotem i bierzemy kolejny element z kolejki
      // problem: być może ten element trzeba odrzucić i od razu zająć się kolejnym... rekurencyjnie
      if (id == persistQueue.head.id) {
        //context.setReceiveTimeout(Duration.Undefined)
        replicator ! SnapshotAck(key, id)
        expectedSeq = id + 1
        handleQueue(persistQueue.tail, replicator)
      } else {
        log.warning(s"Got Persisted message for different (key, id), got ($key, $id), expected (?, ${persistQueue.head.id}})")
      }
    case Snapshot(key, valueOption, seq) =>
      // nie mogę od razu zaaplikować zmian, bo nie wiem czy się seq zgadza, będę to wiedział dopiero jak dotrę do tego elementu w kolejce
      val persist = new Persist(key, valueOption, seq)
      context.become(replicaWaitingForPersistence(persistQueue :+ persist, replicator))
  }

  private def handleQueue(persistQueue: Queue[Persist], replicator: ActorRef): Unit = {
    if (persistQueue.isEmpty) {
      context.become(replica)
    } else {
      val persist = persistQueue.head
      if (persist.id < expectedSeq) {
        replicator ! SnapshotAck(persist.key, persist.id)
        handleQueue(persistQueue.tail, replicator)
      } else {
        if (persist.id == expectedSeq) {
          persist.valueOption match {
            case None => kv = kv - persist.key
            case Some(value) => kv = kv.updated(persist.key, value)
          }
          persistence ! persist
          //context.setReceiveTimeout(persistTimeout)
          context.become(replicaWaitingForPersistence(persistQueue, replicator))
        }
      }
    }
  }
}
