package com.sclasen.akka.kafka

import akka.actor.{Props, Actor, ActorRef, ActorSystem}
import akka.testkit.{TestKit, ImplicitSender}
import concurrent.duration._
import kafka.producer.{KeyedMessage, ProducerConfig, Producer}
import kafka.serializer.DefaultDecoder

import StreamFSM._
import AkkaBatchConsumerSpec._

import org.scalatest._
import kafka.consumer.{Blacklist, Whitelist, TopicFilter}
import akka.pattern._
import kafka.message.MessageAndMetadata
import com.sclasen.akka.kafka.BatchConnectorFSM.{Batch, BatchProcessed}


class AkkaBatchConsumerSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender
with WordSpecLike with Matchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("test"))

  val singleTopic = s"test${System.currentTimeMillis()}"

  val topicFilter = s"filterTest${System.currentTimeMillis()}"

  val producer = kafkaProducer

  val messages = 1000

  import system.dispatcher


  "AkkaConsumer" should {
    "work with a topic" in {
      val receiver = system.actorOf(Props(new TestBatchReciever(testActor)))
      val consumer = new AkkaBatchConsumer(testProps(system, singleTopic, receiver))
      doTest(singleTopic, consumer)
      consumer.stop() pipeTo testActor
      expectMsg(())
    }
  }


  def doTest(topic:String, consumer: AkkaBatchConsumer[Array[Byte], Array[Byte], Array[Byte]]) {
    consumer.start().map {
      _ => testActor ! BatchConnectorFSM.Started
    }
    expectMsg(2 seconds, BatchConnectorFSM.Started)


    (1 to 10).foreach {
      cycle =>
        sendMessages(topic)

        val batch = expectMsgPF(){
          case b:BatchConnectorFSM.Batch[Array[Byte]]  => b
        }

        println("=========>"+batch.items.size)

    }


    (1 to 10).foreach {
      cycle =>
        sendMessages(topic)
    }


    (1 to 10).foreach {
      cycle =>
        val batch = expectMsgPF(){
          case b:BatchConnectorFSM.Batch[Array[Byte]]   => b
        }

        println("=========>"+batch.items.size)


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

  def testProps(system: ActorSystem, topic: String, receiver: ActorRef) = AkkaBatchConsumerProps[Array[Byte],Array[Byte],Array[Byte]](
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
    msgHandler = handler(_)
  )

  def handler(mm:MessageAndMetadata[Array[Byte],Array[Byte]]):Array[Byte] = mm.message()



}

class TestBatchReciever(testActor: ActorRef) extends Actor {
  override def receive = {
    case b:Batch[Array[Byte]]  =>
      sender ! BatchProcessed
      testActor ! b
  }
}

