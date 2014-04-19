package com.sclasen.akka.kafka

import org.scalatest._
import kafka.producer.{ProducerConfig, Producer}
import akka.actor.{Props, Actor, ActorRef, ActorSystem}
import kafka.serializer.DefaultDecoder
import kafka.producer.KeyedMessage
import akka.testkit.{TestKit, ImplicitSender}
import scala.concurrent.duration._
import AkkaConsumerSpec._
import com.sclasen.akka.kafka.StreamFSM.Processed


class AkkaConsumerSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll  {

  def this() = this(ActorSystem("test"))

  val topic = s"test${System.currentTimeMillis()}"

  val producer = kafkaProducer

  val messages = 1000

  "AkkaConsumer" should {
    "work" in {
      val receiver = system.actorOf(Props(new TestReciever(testActor)))
      val consumer = new AkkaConsumer(testProps(system, topic, receiver))
      import system.dispatcher
      consumer.start().map{
        _ => testActor ! ConnectorFSM.Started
      }
      expectMsg(2 seconds, ConnectorFSM.Started)

      (1 to 10).foreach {
       cycle =>
        sendMessages()
        receiveN(messages, 5 seconds)
        consumer.commit().map {
          _ => testActor ! ConnectorFSM.Committed
        }
        expectMsg(10 seconds, ConnectorFSM.Committed)
      }
    }
  }

  def sendMessages() {
    (1 to messages).foreach{
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
  type MsgProducer = Producer[Key,Msg]

  def kafkaProducer = {
    new MsgProducer(new ProducerConfig(kafkaProducerProps))
  }

  def kafkaProducerProps = AkkaConsumer.toProps(
    "metadata.broker.list" -> "localhost:9092",
    "producer.type" -> "sync",
    "request.required.acks" -> "-1"
  )

  def testProps(system:ActorSystem, topic:String, receiver:ActorRef) = AkkaConsumerProps(
    system,
    "localhost:2181",
    topic,
    "consumer-spec",
    2,
    new DefaultDecoder(),
    new DefaultDecoder(),
    receiver, commitAfterMsgCount = 1000
  )

}

class TestReciever(testActor:ActorRef) extends Actor{
  override def receive = {
    case m:Any =>
      sender ! Processed
      testActor ! m
  }
}

