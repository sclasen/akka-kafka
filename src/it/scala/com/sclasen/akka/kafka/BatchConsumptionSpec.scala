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
import scala.collection.mutable
import com.sclasen.akka.kafka.ConnectorFSM.Commit


class BatchConsumptionSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender
with WordSpecLike with Matchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("test"))

  val batchTopic = s"batch${System.currentTimeMillis()}"

  val producer = kafkaProducer

  val messages = 100

  import system.dispatcher


  "AkkaConsumer" should {
    "work with a topic" in {
      val receiver = system.actorOf(Props(new TestBatchReciever(testActor)))
      val consumer = new AkkaConsumer(testProps(system, batchTopic, receiver))
      doTest(batchTopic, consumer, receiver)
      consumer.stop() pipeTo testActor
      expectMsg(())
    }
  }



  def doTest(topic:String, consumer: AkkaConsumer[Array[Byte], Array[Byte]], receiver:ActorRef) {
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
        sendMessages(topic)
        receiveN(messages, 10 seconds)
        consumer.commit().map {
          _ => testActor ! ConnectorFSM.Committed
        }
        receiver ! Commit
        expectMsg(10 seconds, ConnectorFSM.Committed)
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

  def testProps(system: ActorSystem, topic: String, receiver: ActorRef) = AkkaConsumerProps.forSystem(
    system = system,
    connectorActorName = Some("testBatch"),
    zkConnect = "localhost:2181",
    topic = topic,
    group = "batch-spec",
    streams = 2,
    keyDecoder = new DefaultDecoder(),
    msgDecoder = new DefaultDecoder(),
    receiver = receiver,
    commitConfig = CommitConfig(None, None),
    maxInFlightPerStream = 100
  )

}

class TestBatchReciever(testActor: ActorRef) extends Actor {

  var total = 0
  val senders = new mutable.HashMap[ActorRef,Int]()

  override def receive = {
    case Commit =>
      senders.foreach{
        case (sender, acks) =>  (1 to acks).foreach(_ => sender ! Processed)
      }
      senders.clear()
      total = 0
    case m: Any =>
      testActor ! m
      total += 1
      val sent = senders.getOrElseUpdate(sender(), 0)
      senders += (sender() -> (sent + 1))

  }
}

