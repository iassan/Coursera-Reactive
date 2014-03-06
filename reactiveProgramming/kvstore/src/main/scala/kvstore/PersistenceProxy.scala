package kvstore

import akka.actor.{ReceiveTimeout, OneForOneStrategy, Props, Actor}
import akka.actor.SupervisorStrategy.Restart
import kvstore.Persistence.{Persisted, Persist}
import scala.collection.immutable.Queue
import scala.concurrent.duration._

/**
 * @author Jacek Bilski
 * @version $Revision$
 *          $Id$
 */
object PersistenceProxy {
  def props(persistenceProps: Props): Props = Props(new PersistenceProxy(persistenceProps))
}

class PersistenceProxy(val persistenceProps: Props) extends Actor {

  override val supervisorStrategy = OneForOneStrategy() {
    case _: Exception => Restart
  }

  val persistence = context.actorOf(persistenceProps)
  val persistTimeout = 100.milliseconds

  def receive: Receive = waitingForPersist

  def waitingForPersist: Receive = {
    case Persist(key, valueOption, id) =>
      val persist = Persist(key, valueOption, id) // TODO - how to simplify that? _?
      val queue = Queue(persist)
      persistence ! persist
      context.setReceiveTimeout(persistTimeout)
      context.become(waitingForAck(queue), discardOld = false)
  }

  def waitingForAck(queue: Queue[Persist]): Receive = {
    case Persist(key, valueOption, id) =>
      context.become(waitingForAck(queue :+ Persist(key, valueOption, id)))
      // fixme - if those come exactly every ~95%*persistTimeout I will never get a timeout...
    case Persisted(key, id) =>
      context.parent ! Persisted(key, id)
      if (queue.tail.isEmpty) {
        context.setReceiveTimeout(Duration.Undefined)
        context.unbecome()
      } else {
        persistence ! queue.tail.head
        context.setReceiveTimeout(persistTimeout)
        context.become(waitingForAck(queue.tail))
      }
    case ReceiveTimeout =>
      persistence ! queue.head
  }
}
