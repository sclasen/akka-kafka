package com.sclasen.akka.kafka

import akka.actor.{Props, Actor, ActorRef, ActorSystem}
import akka.testkit.{TestKit, ImplicitSender}
import concurrent.duration._
import kafka.producer.{KeyedMessage, ProducerConfig, Producer}
import kafka.serializer.DefaultDecoder

import StreamFSM._
import AkkaConsumerSpec._

import org.scalatest._
import kafka.consumer.{Whitelist, TopicFilter}
import akka.pattern._


class AkkaConsumerSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender
with WordSpecLike with Matchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("test"))

  val topic = s"test${System.currentTimeMillis()}"

  val producer = kafkaProducer

  val messages = 1000

  import system.dispatcher


  "AkkaConsumer" should {
    "work with a topic" in {
      val receiver = system.actorOf(Props(new TestReciever(testActor)))
      val consumer = new AkkaConsumer(testProps(system, topic, receiver))
      doTest(consumer)
      consumer.stop() pipeTo testActor
      expectMsg(())
    }
  }

  "AkkaConsumer with TopicFilter" should {
    "work with a topicFilter" in {
      val receiver = system.actorOf(Props(new TestReciever(testActor)))
      val consumer = new AkkaConsumer(testProps(system, new Whitelist(".*"), receiver))
      doTest(consumer)
      consumer.stop() pipeTo testActor
      expectMsg(())
    }
  }

  def doTest(consumer: AkkaConsumer[Array[Byte], Array[Byte]]) {
    consumer.start().map {
      _ => testActor ! ConnectorFSM.Started
    }
    expectMsg(2 seconds, ConnectorFSM.Started)

    producer.send(new KeyedMessage(topic, 0.toString.getBytes, 0.toString.getBytes))
    receiveOne(2 seconds)
    consumer.commit().map {
      _ => testActor ! ConnectorFSM.Committed
    }
    expectMsg(10 seconds, ConnectorFSM.Committed)

    (1 to 10).foreach {
      cycle =>
        sendMessages()
        receiveN(messages, 10 seconds)
        consumer.commit().map {
          _ => testActor ! ConnectorFSM.Committed
        }
        expectMsg(10 seconds, ConnectorFSM.Committed)
    }
  }


  def sendMessages() {
    (1 to messages).foreach {
      num =>
        producer.send(new KeyedMessage(topic, num.toString.getBytes, num.toString.getBytes))
    }
  }

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

}


object AkkaConsumerSpec {

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

  def testProps(system: ActorSystem, topic: String, receiver: ActorRef) = AkkaConsumerProps.forSystem(
    system = system,
    connectorActorName = Some("testFSM"),
    zkConnect = "localhost:2181",
    topic = topic,
    group = "consumer-spec",
    streams = 2,
    keyDecoder = new DefaultDecoder(),
    msgDecoder = new DefaultDecoder(),
    receiver = receiver
  )

  def testProps(system: ActorSystem, topicFilter: TopicFilter, receiver: ActorRef) = AkkaConsumerProps.forSystemWithFilter(
    system = system,
    connectorActorName = Some("testFSMFilter"),
    zkConnect = "localhost:2181",
    topicFilter = topicFilter,
    group = "consumer-spec",
    streams = 2,
    keyDecoder = new DefaultDecoder(),
    msgDecoder = new DefaultDecoder(),
    receiver = receiver
  )

}

class TestReciever(testActor: ActorRef) extends Actor {
  override def receive = {
    case m: Any =>
      sender ! Processed
      testActor ! m
  }
}

