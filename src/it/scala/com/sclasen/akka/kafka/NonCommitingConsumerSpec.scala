package com.sclasen.akka.kafka

import akka.actor.{Props, ActorRef, ActorSystem}
import akka.testkit.{TestKit, ImplicitSender}
import concurrent.duration._
import kafka.producer.{KeyedMessage, ProducerConfig, Producer}
import kafka.serializer.DefaultDecoder

import AkkaConsumerSpec._

import org.scalatest._
import akka.pattern._


class NonCommitingConsumerSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender
with WordSpecLike with Matchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("test"))

  val singleTopic = s"testNoCommit${System.currentTimeMillis()}"

  val producer = kafkaProducer

  val messages = 10000

  import system.dispatcher


  "AkkaConsumer with Non Committing config" should {
    "work with a topic" in {
      val receiver = system.actorOf(Props(new TestReciever(testActor)))
      val consumer = new AkkaConsumer(testProps(system, singleTopic, receiver))
      doTest(singleTopic, consumer)
      consumer.stop() pipeTo testActor
      expectMsg(())
    }
  }



  def doTest(topic:String, consumer: AkkaConsumer[Array[Byte], Array[Byte]]) {
    consumer.start().map {
      _ => testActor ! ConnectorFSM.Started
    }
    expectMsg(2 seconds, ConnectorFSM.Started)

    producer.send(new KeyedMessage(topic, 0.toString.getBytes, 0.toString.getBytes))
    receiveOne(2 seconds)


    (1 to 10).foreach {
      cycle =>
        sendMessages(topic)
        receiveN(messages, 10 seconds)
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


object NonCommitingConsumerSpec {

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
    receiver = receiver,
    commitConfig = CommitConfig(None, None)
  )


}



