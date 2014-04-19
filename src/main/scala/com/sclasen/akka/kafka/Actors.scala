package com.sclasen.akka.kafka

import kafka.consumer._
import concurrent.duration._
import akka.actor._
import ConnectorFSM._
import StreamFSM._

object ConnectorFSM {

  sealed trait ConnectorState

  case object Committing extends ConnectorState

  case object Receiving extends ConnectorState

  case object Stopped extends ConnectorState

  sealed trait ConnectorProtocol

  case object Start extends ConnectorProtocol

  case class Drained(stream: String) extends ConnectorProtocol

  case object Received extends ConnectorProtocol

  case object Commit extends ConnectorProtocol

}

object StreamFSM {

  sealed trait StreamState

  case object Processing extends StreamState

  case object Full extends StreamState

  case object Draining extends StreamState

  case object Empty extends StreamState

  case object Unused extends StreamState

  sealed trait StreamProtocol

  case object StartProcessing extends StreamProtocol

  case object Drain extends StreamProtocol

  case object Processed extends StreamProtocol

  case object Continue extends StreamProtocol

}


/*
ConnectorFSM manages commits on the kafka ConsumerConnector.

It will commit every `maxSecondsTillCommit` seconds or `maxUncommittedMsgs` processed messages.

Spins up `streams` StreamFSMs which manage the kafka KafkaStream and  ConsumerIterator for the stream.

*/
class ConnectorFSM[Key, Msg](props: AkkaConsumerProps[Key, Msg], connector: ConsumerConnector) extends Actor with FSM[ConnectorState, Int] {

  import props._
  import context.dispatcher

  startWith(Stopped, 0)

  var commitTimeoutCancellable: Option[Cancellable] = None

  def scheduleCommit = {
    commitTimeoutCancellable = Some(context.system.scheduler.scheduleOnce(commitInterval, self, Commit))
  }

  when(Stopped) {
    case Event(Start, _) =>
      val listener = context.system.actorOf(Props(new Actor {
        def receive = {
          case d: DeadLetter ⇒ log.error(d.toString())
          case a: ActorRef => context.watch(a)
          case t: Terminated => log.error("TERMINATED! {}", t)
        }
      }))
      context.system.eventStream.subscribe(listener, classOf[DeadLetter])

      log.info("connectorFSM creating streamFSMs")
      connector.createMessageStreams(Map(topic -> streams)).apply(topic).zipWithIndex.foreach {
        case (stream, index) =>
          val streamActor = context.actorOf(Props(new StreamFSM(stream, maxInFlightPerStream, receiver)), s"stream${index}")
          listener ! streamActor
      }
      context.children.foreach(_ ! Continue)
      scheduleCommit
      sender ! Start
      goto(Receiving) using 0
  }

  when(Receiving) {
    case Event(Received, uncommitted) if uncommitted == commitAfterMsgCount =>
      log.info("commit threshold exceeded, committing {} messages", uncommitted)
      goto(Committing) using 0
    case Event(Received, uncommitted) =>
      log.debug("received uncommitted {}", uncommitted + 1)
      stay using (uncommitted + 1)
    case Event(Commit, uncommitted) =>
      log.info("commit timeout elapsed, committing {} messages", uncommitted)
      goto(Committing) using 0
  }

  onTransition {
    case Receiving -> Committing =>
      commitTimeoutCancellable.foreach(_.cancel())
      context.children.foreach(_ ! Drain)
  }

  onTransition {
    case Committing -> Receiving =>
      scheduleCommit
      context.children.foreach(_ ! StartProcessing)
  }

  when(Committing, stateTimeout = 1 seconds) {
    case Event(Received, drained) =>
      log.info("committed receive, drained: {}", drained)
      stay()
    case Event(StateTimeout, drained) =>
      log.warning(s"waiting to commit, have ${drained} of ${streams} drained")
      context.children.foreach(ref => log.info("{} terminated {}", ref.path, ref.isTerminated))
      context.children.foreach(_ ! Drain)
      stay using (0)
    case Event(Drained(stream), drained) if drained + 1 < context.children.size =>
      log.debug("{} drained: {}", stream, drained + 1)
      stay using (drained + 1)
    case Event(Drained(stream), drained) if drained + 1 == context.children.size =>
      log.debug("{} drained,  finished: {}", stream, drained + 1)
      log.info("completed draining,  committing")
      connector.commitOffsets
      log.info("committed")
      goto(Receiving) using 0
  }

  onTransition(handler _)

  def handler(from: ConnectorState, to: ConnectorState) {
    log.info("connector transition {} -> {}", from, to)
  }

  override def postStop(): Unit = {
    connector.shutdown()
    super.postStop()
  }
}

class StreamFSM[Key, Msg](stream: KafkaStream[Key, Msg], maxOutstanding: Int, receiver: ActorRef) extends Actor with FSM[StreamState, Int] {

  lazy val msgIterator = stream.iterator()
  val conn = context.parent

  def hasNext() = try {
    msgIterator.hasNext()
  } catch {
    case cte: ConsumerTimeoutException => false
  }

  startWith(Processing, 0)

  when(Processing) {
    /* too many outstanding, wait */
    case Event(Continue, outstanding) if outstanding == maxOutstanding =>
      log.debug("{} full", me)
      goto(Full)
    /* ok to process, and msg available */
    case Event(Continue, outstanding) if hasNext() =>
      val msg = msgIterator.next().message()
      conn ! Received
      log.debug("{} received", me)
      receiver ! msg
      self ! Continue
      stay using (outstanding + 1)
    /* no message in iterator and no outstanding. this stream is prob not going to get messages */
    case Event(Continue, outstanding) if outstanding == 0 =>
      log.info("{} empty", me)
      goto(Unused)
    /* no msg in iterator, but have outstanding */
    case Event(Continue, outstanding) =>
      log.info("{} outstanding", me)
      stay()
    /* message processed */
    case Event(Processed, outstanding) =>
      log.debug("{} processed {} out", me, outstanding - 1)
      self ! Continue
      stay using (outstanding - 1)
    /* conn says drain, we have no outstanding */
    case Event(Drain, outstanding) if outstanding == 0 =>
      log.info("{} drain empty", me)
      goto(Empty)
    /* conn says drain, we have outstanding */
    case Event(Drain, outstanding) =>
      log.info("{} drain {}", me, outstanding)
      goto(Draining)
  }

  when(Full) {
    case Event(Continue, outstanding) =>
      log.debug("{} full, {} out", me, outstanding)
      stay()
    case Event(Processed, outstanding) =>
      log.debug("{} full processed, {} out", me, outstanding - 1)
      goto(Processing) using (outstanding - 1)
    case Event(Drain, outstanding) =>
      log.info("{} full, drain {}", me, outstanding)
      goto(Draining)
  }

  when(Draining) {
    /* drained last message */
    case Event(Processed, outstanding) if outstanding == 1 =>
      log.info("{} drained", me)
      goto(Empty) using 0
    /* still draining  */
    case Event(Processed, outstanding) =>
      log.debug("draining: outstanding {}", outstanding - 1)
      stay using (outstanding - 1)
    case Event(Continue, _) =>
      log.debug("{} received a scheduled Continue from Processing which arrived after Drain, ignoring", me)
      stay()
  }

  when(Empty) {
    /* conn says go */
    case Event(StartProcessing, _) =>
      log.debug("{} resume", me)
      goto(Processing) using 0
    case Event(Continue, _) =>
      log.debug("{} received a scheduled Continue from Processing which arrived after Drain, ignoring", me)
      stay()
    case Event(Drain, _) =>
      conn ! Drained(me)
      stay()
  }

  /* we think this stream wont get messages */
  when(Unused) {
    case Event(Drain, _) =>
      log.debug("{} unused drain empty", me)
      goto(Empty)
    case Event(Continue, _) =>
      log.warning("{} unexpected Continue in Unused from {}", me, sender)
      stay()
  }

  onTransition {
    case Empty -> Processing =>
      self ! Continue
  }

  onTransition {
    case Full -> Processing =>
      self ! Continue
  }

  onTransition {
    case Processing -> Full =>
      self ! Continue
  }

  onTransition {
    case _ -> Empty =>
      conn ! Drained(me)
  }

  onTransition {
    case _ -> Draining =>
      log.debug("{} goto draining: outstanding {}", me, nextStateData)
  }

  onTransition(handler _)

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    log.error(reason, "message {}", message)
    super.preRestart(reason, message)
  }

  def handler(from: StreamState, to: StreamState) {
    log.debug("{} transition {} -> {}", me, from, to)
  }

  lazy val me = self.path.name
}