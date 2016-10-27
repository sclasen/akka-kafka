package com.sclasen.akka.kafka

import akka.actor._
import concurrent.duration._
import kafka.consumer._
import kafka.message.MessageAndMetadata
import com.sclasen.akka.kafka.BatchConnectorFSM._
import com.sclasen.akka.kafka.BatchStreamFSM._
import scala.reflect.ClassTag

object BatchConnectorFSM {

  sealed trait BatchConnectorState

  case object WaitingToSendBatch extends BatchConnectorState

  case object WaitingToReceiveBatchProcessed extends BatchConnectorState

  case object Receiving extends BatchConnectorState

  case object Stopped extends BatchConnectorState

  sealed trait BatchConnectorProtocol

  case object Start extends BatchConnectorProtocol

  case class Received[Item](item:Item) extends BatchConnectorProtocol

  case class Batch[Item](items:IndexedSeq[Item]) extends BatchConnectorProtocol

  case object BatchProcessed extends BatchConnectorProtocol

  case object StreamUnused extends BatchConnectorProtocol

  case object Started extends BatchConnectorProtocol

  case object Stop extends BatchConnectorProtocol

  case object SendBatch extends BatchConnectorProtocol

}

object BatchStreamFSM {

  sealed trait BatchStreamState

  case object Processing extends BatchStreamState

  case object Unused extends BatchStreamState

  sealed trait BatchStreamProtocol

  case object Continue extends BatchStreamProtocol

  case object Stop extends BatchStreamProtocol

}

//the data here is the number of Continues outstanding to streams
class BatchConnectorFSM[Key, Msg, Out:ClassTag, BatchOut](props: AkkaBatchConsumerProps[Key, Msg, Out, BatchOut], connector: ConsumerConnector) extends Actor with FSM[BatchConnectorState, Int] {

  import props._
  import context.dispatcher

  startWith(Stopped,0)

  var batch =  new collection.mutable.ArrayBuffer[Out](props.batchSize)

  when(Stopped) {
    case Event(Start, _) =>
      log.info("at=start")
      def startTopic(topic:String){
        connector.createMessageStreams(Map(topic -> streams), props.keyDecoder, props.msgDecoder).apply(topic).zipWithIndex.foreach {
          case (stream, index) =>
            context.actorOf(Props(new BatchStreamFSM(stream, self, props.msgHandler)), s"stream${index}")
        }
      }
      def startTopicFilter(topicFilter:TopicFilter){
        connector.createMessageStreamsByFilter(topicFilter, streams, props.keyDecoder, props.msgDecoder).zipWithIndex.foreach {
          case (stream, index) =>
            context.actorOf(Props(new BatchStreamFSM(stream, self, props.msgHandler)), s"stream${index}")
        }
      }

      topicFilterOrTopic.fold(startTopicFilter, startTopic)

      log.info("at=created-streams")
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
      debugRec(StreamUnused, batch.size, outstanding)
      stay using outstanding -1
    case Event(Received(b:Out), outstanding) =>
      batch += b
      debugRec(Received, batch.size, outstanding)
      goto(WaitingToSendBatch) using outstanding - 1
    case Event(StateTimeout, outstanding) if batch.size == 0 =>
      debugRec(StateTimeout, 0, outstanding)
      log.debug("at=nothing-to-commit outstanding={} batch-size={}", outstanding, batch.size)
      context.children.foreach(_ ! Continue)
      stay() using outstanding + props.streams
    case Event(StateTimeout, outstanding) =>
      debugRec(StateTimeout, 0, outstanding)
      log.info("at=receive-timeout outstanding={} batch-size={}", outstanding, batch.size)
      goto(WaitingToSendBatch)
  }

  when(WaitingToSendBatch, stateTimeout = 1 seconds) {
    case Event(SendBatch, 0) =>
      sendBatch()
    case Event(StateTimeout, outstanding) =>
      log.warning("state={} msg={} outstanding={} streams={}", WaitingToSendBatch, StateTimeout, outstanding, streams)
      stay()
    case Event(StreamUnused, outstanding) if outstanding == 1 =>
      sendBatch()
    case Event(StreamUnused, outstanding) =>
      stay using outstanding - 1
    case Event(Received(b:Out), outstanding) if outstanding == 1 =>
      batch += b
      sendBatch()
    case Event(Received(b:Out), outstanding) =>
      batch += b
      stay using outstanding -1
  }

  when(WaitingToReceiveBatchProcessed, stateTimeout = 1 seconds){
    case Event(StateTimeout, outstanding) =>
      log.warning("state={} msg={}  streams={}", WaitingToReceiveBatchProcessed, StateTimeout, streams)
      stay()
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

  onTransition{
    //when we transistion fron Receiving to Committing with 0 outstanding, immeduately send batch
    case Receiving -> WaitingToSendBatch if nextStateData == 0 => self ! SendBatch
  }


  def sendBatch() = {
    val toSend = batch
    batch =  new collection.mutable.ArrayBuffer[Out](props.batchSize)
    props.receiver ! batchHandler(toSend)
    goto(WaitingToReceiveBatchProcessed) using 0
  }

  def debugRec(msg:AnyRef, batch:Int, out:Int) = log.debug("state={} msg={} batch={} out={}", Receiving, msg,  batch, out)

  def debugCommit(msg:AnyRef, stream:String, drained:Int) = log.debug("state={} msg={} drained={}", WaitingToSendBatch, msg,  drained)

}


class BatchStreamFSM[Key, Msg, Out:ClassTag](stream: KafkaStream[Key, Msg],receiver: ActorRef, msgHandler: (MessageAndMetadata[Key,Msg]) => Out) extends Actor with FSM[BatchStreamState, Int] {

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

