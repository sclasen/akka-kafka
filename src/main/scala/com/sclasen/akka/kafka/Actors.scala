package com.sclasen.akka.kafka

import akka.actor._
import concurrent.duration._
import kafka.consumer._

import com.sclasen.akka.kafka.ConnectorFSM._
import StreamFSM._
import scala.Some
import com.sclasen.akka.kafka.ConnectorFSM.Drained
import akka.actor.DeadLetter
import akka.actor.Terminated
import kafka.message.MessageAndMetadata

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

  case object Started extends ConnectorProtocol

  case object Committed extends ConnectorProtocol

  case object Stop extends ConnectorProtocol

}

object StreamFSM {

  sealed trait StreamState

  case object Processing extends StreamState

  case object Full extends StreamState

  case object Draining extends StreamState

  case object Empty extends StreamState

  case object Unused extends StreamState

  case object FlattenContinue extends StreamState

  sealed trait StreamProtocol

  case object StartProcessing extends StreamProtocol

  case object Drain extends StreamProtocol

  case object Processed extends StreamProtocol

  case object Continue extends StreamProtocol

  case object Stop extends StreamProtocol

}

class ConnectorFSM[Key, Msg](props: AkkaConsumerProps[Key, Msg], connector: ConsumerConnector) extends Actor with FSM[ConnectorState, Int] {

  import props._
  import context.dispatcher

  startWith(Stopped, 0)

  var commitTimeoutCancellable: Option[Cancellable] = None
  var committer: Option[ActorRef] = None

  def scheduleCommit = {
    commitTimeoutCancellable = commitConfig.commitInterval.map(i => context.system.scheduler.scheduleOnce(i, self, Commit))
  }

  when(Stopped) {
    case Event(Start, _) =>
      log.info("at=start")
      def startTopic(topic:String){
        connector.createMessageStreams(Map(topic -> streams), props.keyDecoder, props.msgDecoder).apply(topic).zipWithIndex.foreach {
          case (stream, index) =>
            context.actorOf(Props(new StreamFSM(stream, maxInFlightPerStream, receiver, msgHandler, unusedTimeout)), s"stream${index}")
        }
      }
      def startTopicFilter(topicFilter:TopicFilter){
        connector.createMessageStreamsByFilter(topicFilter, streams, props.keyDecoder, props.msgDecoder).zipWithIndex.foreach {
          case (stream, index) =>
            context.actorOf(Props(new StreamFSM(stream, maxInFlightPerStream, receiver, msgHandler, unusedTimeout)), s"stream${index}")
        }
      }

      topicFilterOrTopic.fold(startTopicFilter, startTopic)

      log.info("at=created-streams")
      context.children.foreach(_ ! Continue)
      scheduleCommit
      sender ! Started
      goto(Receiving) using 0
  }

  when(Receiving) {
    case Event(Received, uncommitted) if commitConfig.commitAfterMsgCount.exists(_ == uncommitted) =>
      debugRec(Received, uncommitted)
      goto(Committing) using 0
    case Event(Received, uncommitted) =>
      debugRec(Received, uncommitted + 1)
      stay using (uncommitted + 1)
    case Event(Commit, uncommitted) =>
      debugRec(Commit, uncommitted)
      committer = Some(sender)
      goto(Committing) using 0
    case Event(Committed, uncommitted) =>
      debugRec(Committed, uncommitted)
      stay()
    case Event(d@Drained(s), uncommitted) =>
      debugRec(d, uncommitted) /*when we had to send more than 1 Drain msg to streams, we get these*/
      stay()
  }

  onTransition {
    case Receiving -> Committing =>
      log.info("at=transition from={} to={} uncommitted={}", Receiving, Committing, stateData)
      commitTimeoutCancellable.foreach(_.cancel())
      context.children.foreach(_ ! Drain)
  }

  onTransition {
    case Committing -> Receiving =>
      log.info("at=transition from={} to={}", Committing, Receiving)
      scheduleCommit
      committer.foreach(_ ! Committed)
      committer = None
      context.children.foreach(_ ! StartProcessing)
  }

  when(Committing, stateTimeout = 1 seconds) {
    case Event(Received, drained) =>
      debugCommit(Received, "stream", drained)
      stay()
    case Event(StateTimeout, drained) =>
      log.warning("state={} msg={} drained={} streams={}", Committing, StateTimeout, drained, streams)
      context.children.foreach(_ ! Drain)
      stay using (0)
    case Event(d@Drained(stream), drained) if drained + 1 < context.children.size =>
      debugCommit(d, stream, drained + 1)
      stay using (drained + 1)
    case Event(d@Drained(stream), drained) if drained + 1 == context.children.size =>
      debugCommit(d, stream, drained + 1)
      log.info("at=drain-finished")
      connector.commitOffsets
      log.info("at=committed-offsets")
      goto(Receiving) using 0
  }

  whenUnhandled{
    case Event(ConnectorFSM.Stop, _) =>
      connector.shutdown()
      sender() ! ConnectorFSM.Stop
      context.children.foreach(_ ! StreamFSM.Stop)
      stop()
  }

  def debugRec(msg:AnyRef, uncommitted:Int) = log.debug("state={} msg={} uncommitted={}", Receiving, msg,  uncommitted)

  def debugCommit(msg:AnyRef, stream:String, drained:Int) = log.debug("state={} msg={} drained={}", Committing, msg,  drained)

}

class StreamFSM[Key, Msg](stream: KafkaStream[Key, Msg], maxOutstanding: Int, receiver: ActorRef, msgHandler: (MessageAndMetadata[Key,Msg]) => Any, unusedTimeout: FiniteDuration) extends Actor with FSM[StreamState, Int] {

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
       debug(Processing, Continue, outstanding)
       goto(Full)
    /* ok to process, and msg available */
    case Event(Continue, outstanding) if hasNext() =>
      val msg = msgHandler(msgIterator.next())
      conn ! Received
      debug(Processing, Continue, outstanding +1)
      receiver ! msg
      self ! Continue
      stay using (outstanding + 1)
    /* no message in iterator and no outstanding. this stream is prob not going to get messages */
    case Event(Continue, outstanding) if outstanding == 0 =>
      debug(Processing, Continue, outstanding)
      goto(Unused)
    /* no msg in iterator, but have outstanding */
    case Event(Continue, outstanding) =>
      debug(Processing, Continue, outstanding)
      goto(FlattenContinue)
    /* message processed */
    case Event(Processed, outstanding) =>
      debug(Processing, Processed, outstanding -1)
      self ! Continue
      stay using (outstanding - 1)
    /* conn says drain, we have no outstanding */
    case Event(Drain, outstanding) if outstanding == 0 =>
      debug(Processing, Drain, outstanding)
      goto(Empty)
    /* conn says drain, we have outstanding */
    case Event(Drain, outstanding) =>
      debug(Processing, Drain, outstanding)
      goto(Draining)
  }

  /*
  We go into FlattenContinue when we poll the iterator and there are no msgs but we have in-flight messages.
  We anticipate that future calls to the iterator will timeout as well, so instead of polling once for every Continue
  that is in the mailbox, we just drain them out, and wait for the in-flight messages to drain.
  once we do that, we go back to processing. This speeds up commit alot in the cycle where a stream goes empty.
  */
  when(FlattenContinue){
    case Event(Continue, outstanding) =>
      debug(FlattenContinue, Continue, outstanding)
      stay()
    case Event(Processed, outstanding) if outstanding == 1 =>
      debug(FlattenContinue, Processed, outstanding -1)
      self ! Continue
      goto(Processing) using (outstanding - 1)
    case Event(Processed, outstanding) =>
      debug(FlattenContinue, Processed, outstanding -1)
      stay using (outstanding - 1)
    case Event(Drain, outstanding) if outstanding == 0 =>
      debug(FlattenContinue, Drain, outstanding)
      goto(Empty)
    case Event(Drain, outstanding) =>
      debug(FlattenContinue, Drain, outstanding)
      goto(Draining)
  }

  when(Full) {
    case Event(Continue, outstanding) =>
      debug(Full, Continue, outstanding)
      stay()
    case Event(Processed, outstanding) =>
      debug(Full, Processed, outstanding - 1)
      goto(Processing) using (outstanding - 1)
    case Event(Drain, outstanding) =>
      debug(Full, Drain, outstanding)
      goto(Draining)
  }

  when(Draining) {
    /* drained last message */
    case Event(Processed, outstanding) if outstanding == 1 =>
      debug(Draining, Processed, outstanding)
      goto(Empty) using 0
    /* still draining  */
    case Event(Processed, outstanding) =>
      debug(Draining, Processed, outstanding)
      stay using (outstanding - 1)
    case Event(Continue, outstanding) =>
      debug(Draining, Continue, outstanding)
      stay()
    case Event(Drain, outstanding) =>
      debug(Draining, Drain, outstanding)
      stay()
  }

  when(Empty) {
    /* conn says go */
    case Event(StartProcessing, outstanding) =>
      debug(Unused, Drain, outstanding)
      goto(Processing) using 0
    case Event(Continue, outstanding) =>
      debug(Unused, Drain, outstanding)
      stay()
    case Event(Drain, _) =>
      conn ! Drained(me)
      stay()
  }

  /* we think this stream might not get any more messages but you can configure a timeout
   * so it can ask for the next message on the kafka iterator */
  when(Unused, stateTimeout = unusedTimeout) {
    case Event(Drain, outstanding) =>
      debug(Unused, Drain, outstanding)
      goto(Empty)
    case Event(Continue, outstanding) =>
      debug(Unused, Continue, outstanding)
      goto(Processing)
    case Event(StateTimeout, outstanding) =>
      debug(Unused, Continue, outstanding)
      self ! Continue
      goto(Processing) using outstanding



  }

  whenUnhandled{
    case Event(StreamFSM.Stop, _) =>
      stop()
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
      log.debug("stream={} at=Drained", me)
      conn ! Drained(me)
  }

  onTransition(handler _)

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    log.error(reason, "message {}", message)
    super.preRestart(reason, message)
  }

  def handler(from: StreamState, to: StreamState) {
    log.debug("stream={} at=transition from={} to={}", me, from, to)
  }

  def debug(state:StreamState, msg:StreamProtocol, outstanding:Int) = log.debug("stream={} state={} msg={} outstanding={}", me, state, msg,  outstanding)

  lazy val me = self.path.name
}
