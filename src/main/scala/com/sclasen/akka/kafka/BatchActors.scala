package com.sclasen.akka.kafka

import akka.actor._
import concurrent.duration._
import kafka.consumer._
import akka.actor.DeadLetter
import akka.actor.Terminated
import kafka.message.MessageAndMetadata
import com.sclasen.akka.kafka.BatchConnectorFSM._
import com.sclasen.akka.kafka.BatchStreamFSM._

object BatchConnectorFSM {

  sealed trait BatchConnectorState

  case object Committing extends BatchConnectorState

  case object Receiving extends BatchConnectorState

  case object Stopped extends BatchConnectorState

  sealed trait BatchConnectorProtocol

  case object Start extends BatchConnectorProtocol

  case class Received[Item](item:Item) extends BatchConnectorProtocol

  case class Batch[Item](items:IndexedSeq[Item]) extends BatchConnectorProtocol

  case object BatchProcessed extends BatchConnectorProtocol

  case object StreamUnused extends BatchConnectorProtocol

  case object Commit extends BatchConnectorProtocol

  case object Started extends BatchConnectorProtocol

  case object Committed extends BatchConnectorProtocol

  case object Stop extends BatchConnectorProtocol

}

object BatchStreamFSM {

  sealed trait BatchStreamState

  case object Processing extends BatchStreamState

  case object Waiting extends BatchStreamState

  case object Unused extends BatchStreamState

  sealed trait BatchStreamProtocol

  case object StartProcessing extends BatchStreamProtocol

  case object Continue extends BatchStreamProtocol

  case object Stop extends BatchStreamProtocol

}

//the data here is the number of Continues outstanding to streams
class BatchConnectorFSM[Key, Msg, Out](props: AkkaBatchConsumerProps[Key, Msg, Out], connector: ConsumerConnector) extends Actor with FSM[BatchConnectorState, Int] {

  import props._
  import context.dispatcher

  startWith(Stopped,0)

  var batch =  new collection.mutable.ArrayBuffer[Out](props.batchSize)

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

      log.info("at=start")
      def startTopic(topic:String){
        connector.createMessageStreams(Map(topic -> streams), props.keyDecoder, props.msgDecoder).apply(topic).zipWithIndex.foreach {
          case (stream, index) =>
            val streamActor = context.actorOf(Props(new BatchStreamFSM(stream, self, props.msgHandler)), s"stream${index}")
            listener ! streamActor
        }
      }
      def startTopicFilter(topicFilter:TopicFilter){
        connector.createMessageStreamsByFilter(topicFilter, streams, props.keyDecoder, props.msgDecoder).zipWithIndex.foreach {
          case (stream, index) =>
            val streamActor = context.actorOf(Props(new BatchStreamFSM(stream, self, props.msgHandler)), s"stream${index}")
            listener ! streamActor
        }
      }

      topicFilterOrTopic.fold(startTopicFilter, startTopic)

      log.info("at=created-streams")
      context.children.foreach(println)
      context.children.foreach(_ ! Continue)
      sender ! Started
      goto(Receiving) using context.children.size
  }

  when(Receiving, props.batchTimeout.duration) {
    case Event(Received(b:Out), outstanding) if batch.size < props.batchSize - props.streams =>
      batch += b
      debugRec(Received, batch.size, outstanding)
      sender() ! Continue
      stay()
    case Event(StreamUnused, outstanding) =>
      stay using outstanding -1
    case Event(Received(b:Out), outstanding) =>
      batch += b
      debugRec(Received, batch.size, outstanding)
      goto(Committing) using outstanding - 1
    case Event(StateTimeout, outstanding) =>
      goto(Committing)
  }

  when(Committing, stateTimeout = 1 seconds) {
    case Event(StateTimeout, outstanding) =>
      log.warning("state={} msg={} outstanding={} streams={}", Committing, StateTimeout, outstanding, streams)
      stay()
    case Event(StreamUnused, outstanding) if outstanding == 1 =>
      log.info("at=drain-finised")
      sendBatch()
      log.info("at=committed-offsets")
      stay() using 0
    case Event(StreamUnused, outstanding) =>
      stay using outstanding - 1
    case Event(Received(b:Out), outstanding) if outstanding == 1 =>
      batch += b
      log.info("at=drain-finised")
      sendBatch()
      log.info("at=committed-offsets")
      stay using 0
    case Event(Received(b:Out), outstanding) =>
      batch += b
      stay using outstanding -1
    case Event(BatchProcessed, _) =>
      connector.commitOffsets
      batch.clear()
      context.children.foreach(_ ! Continue)
      goto(Receiving) using context.children.size
  }

  whenUnhandled{
    case Event(BatchConnectorFSM.Stop, _) =>
      connector.shutdown()
      sender() ! BatchConnectorFSM.Stop
      context.children.foreach(_ ! BatchStreamFSM.Stop)
      stop()
  }

  def sendBatch() = {
    val toSend = batch
    batch =  new collection.mutable.ArrayBuffer[Out](props.batchSize)
    props.receiver ! Batch(toSend)
  }

  def debugRec(msg:AnyRef, batch:Int, out:Int) = log.debug("state={} msg={} batch={} out={}", Receiving, msg,  batch, out)

  def debugCommit(msg:AnyRef, stream:String, drained:Int) = log.debug("state={} msg={} drained={}", Committing, msg,  drained)

}


class BatchStreamFSM[Key, Msg, Out](stream: KafkaStream[Key, Msg],receiver: ActorRef, msgHandler: (MessageAndMetadata[Key,Msg]) => Out) extends Actor with FSM[BatchStreamState, Int] {

  lazy val msgIterator = stream.iterator()
  val conn = context.parent

  def hasNext() = try {
    msgIterator.hasNext()
  } catch {
    case cte: ConsumerTimeoutException => false
  }

  startWith(Processing, 0)

  when(Processing) {
    /* ok to process, and msg available */
    case Event(Continue, outstanding) if hasNext() =>
      val msg = msgHandler(msgIterator.next())
      debug(Processing, Continue, outstanding)
      conn ! Received(msg)
      stay()
    /* no message in iterator and no outstanding. this stream is prob not going to get messages */
    case Event(Continue, outstanding) if outstanding == 0 =>
      debug(Processing, Continue, outstanding)
      conn ! StreamUnused
      goto(Unused)
  }


  /* we think this stream wont get messages */
  when(Unused) {
    case Event(Continue, outstanding) =>
      debug(Unused, Continue, outstanding)
      goto(Processing)
  }

  whenUnhandled{
    case Event(BatchStreamFSM.Stop, _) =>
      stop()
  }

  onTransition {
    case Unused -> Processing =>
      self ! Continue
  }


  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    log.error(reason, "message {}", message)
    super.preRestart(reason, message)
  }

  def debug(state:BatchStreamState, msg:BatchStreamProtocol, outstanding:Int) = log.debug("stream={} state={} msg={} outstanding={}", me, state, msg,  outstanding)

  lazy val me = self.path.name
}

