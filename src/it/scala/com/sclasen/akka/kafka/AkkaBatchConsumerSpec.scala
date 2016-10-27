package com.sclasen.akka.kafka

import akka.actor.{Props, Actor, ActorRef, ActorSystem}
import akka.testkit.{TestKit, ImplicitSender}
import concurrent.duration._
import kafka.producer.{KeyedMessage, ProducerConfig, Producer}
import kafka.serializer.DefaultDecoder

import AkkaBatchConsumerSpec._

import org.scalatest._
import akka.pattern._
import kafka.message.MessageAndMetadata
import com.sclasen.akka.kafka.BatchConnectorFSM.{BatchProcessed}


class AkkaBatchConsumerSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender
with WordSpecLike with Matchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("batch-test"))

  val singleTopic = s"batchTest${System.currentTimeMillis()}"

  val topicFilter = s"batchFilterTest${System.currentTimeMillis()}"

  val producer = kafkaProducer

  val messages = 1000

  import system.dispatcher


  "AkkaConsumer" should {
    "work with a topic" in {
      val receiver = system.actorOf(Props(new TestBatchReceiver(testActor)))
      val consumer = new AkkaBatchConsumer(testProps(system, singleTopic, receiver))
      doTest(singleTopic, consumer)
      consumer.stop() pipeTo testActor
      expectMsg(())
    }
  }


  def doTest(topic:String, consumer: AkkaBatchConsumer[Array[Byte], Array[Byte], Array[Byte],SpecificallyTypedBatch]) {
    consumer.start().map {
      _ => testActor ! BatchConnectorFSM.Started
    }
    expectMsg(2 seconds, BatchConnectorFSM.Started)

    /*test that we are ok after a receive timeout*/
    expectNoMsg()

    (1 to 10).foreach {
      cycle =>
        sendMessages(topic)

        val batch = expectMsgPF(){
          case s:SpecificallyTypedBatch if s.msgs.size > 0  => s
        }

    }


    (1 to 10).foreach {
      cycle =>
        sendMessages(topic)
    }


    (1 to 10).foreach {
      cycle =>
        val batch = expectMsgPF(){
          case s:SpecificallyTypedBatch if s.msgs.size > 0  => s
        }

    }
  }


  def sendMessages(topic:String) {
    (1 to messages).foreach {
      num =>
        producer.send(new KeyedMessage(topic, num.toString.getBytes, num.toString.getBytes))
    }
  }

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

}


object AkkaBatchConsumerSpec {

  type Key = Array[Byte]
  type Msg = Array[Byte]
  type MsgProducer = Producer[Key, Msg]

  def kafkaProducer = {
    new MsgProducer(new ProducerConfig(kafkaProducerProps))
  }

  def kafkaProducerProps = AkkaConsumer.toProps(collection.mutable.Set(
    "metadata.broker.list" -> "localhost:9092",
    "producer.type" -> "sync",
    "request.required.acks" -> "-1")
  )

  def testProps(system: ActorSystem, topic: String, receiver: ActorRef) = AkkaBatchConsumerProps[Array[Byte],Array[Byte],Array[Byte],SpecificallyTypedBatch](
    system = system,
    actorRefFactory = system,
    connectorActorName = Some("testBatchFSM"),
    zkConnect = "localhost:2181",
    topicFilterOrTopic = Right(topic),
    group = "consumer-spec",
    streams = 2,
    keyDecoder = new DefaultDecoder(),
    msgDecoder = new DefaultDecoder(),
    receiver = receiver,
    msgHandler = handler(_),
    batchHandler = batch(_)
  )

  case class SpecificallyTypedBatch(msgs: IndexedSeq[Array[Byte]])

  def handler(mm:MessageAndMetadata[Array[Byte],Array[Byte]]):Array[Byte] = mm.message()

  def batch(msgs:IndexedSeq[Array[Byte]]):SpecificallyTypedBatch = SpecificallyTypedBatch(msgs)



}

class TestBatchReceiver(testActor: ActorRef) extends Actor {
  override def receive = {
    case s: SpecificallyTypedBatch =>
      sender ! BatchProcessed
      testActor ! s
  }
}

