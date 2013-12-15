/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef

    def id: Int

    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection */
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply

  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor with ActorLogging {

  import BinaryTreeSet._
  import BinaryTreeNode.{CopyTo, CopyFinished}

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case Contains(requester, id, elem) =>
      //log.info(s"Got contains message, id: $id, elem: $elem")
      root ! new Contains(requester, id, elem)
    case Insert(requester, id, elem) =>
      //log.info(s"Got insert message, id: $id, elem: $elem")
      root ! new Insert(requester, id, elem)
    case Remove(requester, id, elem) =>
      //log.info(s"Got remove message, id: $id, elem: $elem")
      root ! new Remove(requester, id, elem)
    case GC =>
      //log.warning("Got GC message")
      val newRoot = createRoot
      root ! new CopyTo(newRoot)
      context.become(garbageCollecting(newRoot), discardOld = true)
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case GC => // ignore
    case Contains(requester, id, elem) => enqueueOperation(new Contains(requester, id, elem))
    case Insert(requester, id, elem) => enqueueOperation(new Insert(requester, id, elem))
    case Remove(requester, id, elem) => enqueueOperation(new Remove(requester, id, elem))
    case CopyFinished =>
      //log.info(s"Old root reports end of copying, sending PoisonPill")
      //root ! PoisonPill
      root = newRoot
      for (operation <- pendingQueue) {
        //log.info(s"Dequeueing operation, id: ${operation.id}, elem: ${operation.elem}")
        self ! operation
      }
      pendingQueue = Queue.empty[Operation]
      context.become(normal)
  }

  def enqueueOperation(operation: Operation): Unit = {
    //log.info(s"Enqueueing new operation to run: id: ${operation.id}, elem: ${operation.elem}")
    pendingQueue = pendingQueue.enqueue(operation)
  }
}

object BinaryTreeNode {

  trait Position

  case object Left extends Position

  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)

  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode], elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor with ActorLogging {

  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case Contains(requester, id, lookupElem) => contains(requester, id, lookupElem)
    case Insert(requester, id, newElem) => insert(requester, id, newElem)
    case Remove(requester, id, elemToRemove) => remove(requester, id, elemToRemove)
    case CopyTo(treeNode) =>
      //log.info(s"got copyTo request")
      var awaitingReplies = Set.empty[ActorRef]
      if (!removed) {
        //log.info(s"this node is not removed, scheduling insertion, elem: $elem")
        treeNode ! new Insert(self, elem, elem)
      }
      if (subtrees.contains(Left)) {
        //log.info(s"left subtree not empty, copying, elem: $elem")
        subtrees(Left) ! new CopyTo(treeNode)
        awaitingReplies = awaitingReplies + subtrees(Left)
      }
      if (subtrees.contains(Right)) {
        //log.info(s"right subtree not empty, copying, elem: $elem")
        subtrees(Right) ! new CopyTo(treeNode)
        awaitingReplies = awaitingReplies + subtrees(Right)
      }
      if (awaitingReplies.isEmpty && removed) {
        context.parent ! CopyFinished
      } else
        context.become(copying(awaitingReplies, insertConfirmed = removed), discardOld = true)
  }

  private def contains(requester: ActorRef, id: Int, lookupElem: Int): Unit = {
    if (lookupElem == elem) {
      //log.info(s"contains(id: $id, lookupElem: $lookupElem), found self ($elem), result: ${!removed}")
      requester ! new ContainsResult(id, !removed)
    } else {
      if (lookupElem > elem) {
        if (subtrees.contains(Right)) {
          //log.info()
          subtrees(Right) ! new Contains(requester, id, lookupElem)
        } else {
          //log.info(s"contains(id: $id, lookupElem: $lookupElem), not found")
          requester ! new ContainsResult(id, false)
        }
      } else {
        if (subtrees.contains(Left)) {
          subtrees(Left) ! new Contains(requester, id, lookupElem)
        } else {
          //log.info(s"contains(id: $id, lookupElem: $lookupElem), not found")
          requester ! new ContainsResult(id, false)
        }
      }
    }
  }

  private def insert(requester: ActorRef, id: Int, newElem: Int): Unit = {
    // do we need to care about new element being equal to an existing one?
    if (newElem == elem && removed) {
      removed = false
      requester ! new OperationFinished(id)
    }
    if (newElem > elem) {
      if (subtrees.contains(Right))
        subtrees(Right) ! new Insert(requester, id, newElem)
      else {
        val right = context.actorOf(props(newElem, initiallyRemoved = false))
        subtrees = subtrees.updated(Right, right)
        //log.info(s"Created new node for insert, id: $id, newElem: $newElem, name: ${right.path}")
        requester ! new OperationFinished(id)
      }
    } else {
      if (subtrees.contains(Left))
        subtrees(Left) ! new Insert(requester, id, newElem)
      else {
        val left = context.actorOf(props(newElem, initiallyRemoved = false))
        subtrees = subtrees.updated(Left, left)
        //log.info(s"Created new node for insert, id: $id, newElem: $newElem, name: ${left.path}")
        requester ! new OperationFinished(id)
      }
    }
  }

  private def remove(requester: ActorRef, id: Int, elemToRemove: Int): Unit = {
    if (elemToRemove == elem) {
      removed = true
      //log.info(s"Node marked as removed, id: $id, elemToRemove: $elemToRemove")
      requester ! new OperationFinished(id)
    } else {
      if (elemToRemove > elem) {
        if (subtrees.contains(Right))
          subtrees(Right) ! new Remove(requester, id, elemToRemove)
        else {
          //log.info(s"remove(id: $id, elemToRemove: $elemToRemove), not found")
          requester ! new OperationFinished(id)
        }
      } else {
        if (subtrees.contains(Left))
          subtrees(Left) ! new Remove(requester, id, elemToRemove)
        else {
          //log.info(s"remove(id: $id, elemToRemove: $elemToRemove), not found")
          requester ! new OperationFinished(id)
        }
      }
    }
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case OperationFinished(id) =>
      //log.info(s"while copying insert confirmed, id: $id, still waiting for ${expected.size} child nodes to finish")
      if (id == elem) {
        if (expected.isEmpty) {
          //log.info(s"while copying no more things to do, id: $elem")
          context.parent ! CopyFinished
          //context.become(normal)
          context.stop(self)
        } else
          context.become(copying(expected, insertConfirmed = true), discardOld = true)
      }
    case CopyFinished =>
      //log.info(s"while copying copy of subnode finished, node: ${sender.path}, still waiting for ${expected.size} child nodes to finish, waiting for insert confirmation: ${!insertConfirmed}")
      val newExpected = expected - sender
      //sender ! PoisonPill
      if (newExpected.isEmpty && insertConfirmed) {
        //log.info(s"while copying no more things to do, id: $elem")
        context.parent ! CopyFinished
        //context.become(normal)
        context.stop(self)
      } else
        context.become(copying(newExpected, insertConfirmed), discardOld = true)
  }
}
