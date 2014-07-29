package com.ak

import akka.actor._

import scala.collection.mutable
import scala.concurrent.duration._

/**
 * Created by akodali on 7/29/14.
 */

object WorkerPatternProtocol {
  case class Work[Id, W](id: Id, work: W)
  case class WorkAck[Id](id: Id)
  case class WorkDone[Id](id: Id)

  case object Register
  case object RegisterAck
}

class NodeSupervisorActor[Id, W](masterProxy: ActorRef, workerProps: Props) extends Actor {
  import WorkerPatternProtocol._

  var master: ActorRef = _
  var idWorkerMap = Map.empty[Id, ActorRef]
  var idCount = Map.empty[Id, Int]

  override def preStart(): Unit =  {
    context.setReceiveTimeout(10 seconds)
    masterProxy ! Register
  }

  def receive = waitingForMaster()

  def waitingForMaster(): Receive = {
    case RegisterAck =>
      context.setReceiveTimeout(Duration.Undefined)
      master = sender
      context.watch(master)

    case ReceiveTimeout =>
      masterProxy ! Register
  }

  def ready(): Receive = {
    case RegisterAck =>
      //ignore

    case Terminated(ref) if master == ref =>
      throw new Exception(s"Master Died!!! Triggering restart $self")

    case w @ Work(id: Id, work: W) if idWorkerMap.contains(id) =>
      idWorkerMap(id) ! w
      idCount += (id -> (idCount(id) + 1))

    case w @ Work(id: Id, work: W) if !idWorkerMap.contains(id) =>
      val worker = context.actorOf(workerProps, id.toString)
      idWorkerMap += id -> worker
      idCount += id -> 0
      worker ! w

    case wd @ WorkDone(id: Id) if idCount(id) > 1 =>
      idCount += (id -> (idCount(id) - 1 ))
      master ! wd

    case wd @ WorkDone(id: Id) if idCount(id) == 1 =>
      idCount -= id
      idWorkerMap -= id
      master ! wd
  }
}

object Master {
  case object MasterUp
  case object MasterDown
}

class Master[Id, W](notifyRef: Option[ActorRef]) extends Stash {
  import WorkerPatternProtocol._

  val nodeSupervisors: mutable.Queue[ActorRef] = mutable.Queue.empty[ActorRef]
  var idNodeMap = Map.empty[Id, ActorRef]
  var idCount = Map.empty[Id, Int]
  var idSenderMap = Map.empty[Id, List[ActorRef]]

  private val circular: Iterator[ActorRef] = Iterator.continually(nodeSupervisors).flatten

  override def postStop(): Unit = notifyRef.map(_ ! Master.MasterDown)

  def receive: Receive = waitingForNodes()

  def waitingForNodes(): Receive = {
    case Register =>
      context.watch(sender)
      nodeSupervisors.enqueue(sender)
      sender ! RegisterAck
      context.become(ready())
      notifyRef.map(_ ! Master.MasterUp)
      unstashAll()

    case w: Work =>
      stash()
  }

  def ready(): Receive = {
    case w @ Work(id: Id, work: W) if idNodeMap.contains(id) =>
      idSenderMap += (id -> (idSenderMap(id) :+ sender))
      idCount += (id -> (idCount(id) + 1))
      idNodeMap(id) forward w

    case w @ Work(id: Id, work: W) if !idNodeMap.contains(id) =>
      idSenderMap += id -> List(sender)
      val node: ActorRef = circular.take(1).next
      idCount += (id -> 0)
      idNodeMap += (id -> node)
      node forward w

    case WorkDone(id: Id) if idCount(id) == 1 =>
      idSenderMap -= id
      idCount -= id
      idNodeMap -= id

    case WorkDone(id: Id) if idCount(id) > 1 =>
      idCount += (id -> (idCount(id) + 1))

    case Register if !nodeSupervisors.contains(sender) =>
      context.watch(sender)
      nodeSupervisors.enqueue(sender)
      sender ! RegisterAck

    case Register if nodeSupervisors.contains(sender) =>
      // Do nothing

    case Terminated(nodeRef) if nodeSupervisors.contains(nodeRef) =>
      nodeSupervisors.dequeueFirst( _ == nodeRef)
      idNodeMap.filter{case (_, ref) => ref == nodeRef}.keys.map(id => idCount -= id)
      idNodeMap = idNodeMap.filterNot{case (_, ref) => ref == nodeRef}
      if(nodeSupervisors.size == 0 ) {
        notifyRef.map(_ ! Master.MasterDown)
        context.become(waitingForNodes())
      }

    case Terminated(nodeRef) if !nodeSupervisors.contains(nodeRef) =>
      // Weird do nothing

  }


}
