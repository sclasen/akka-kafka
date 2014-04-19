package com.sclasen.akka.kafka

import akka.actor.{Props, ActorRef, ActorSystem}
import scala.concurrent.duration._
import kafka.consumer.{ConsumerConfig, Consumer, KafkaStream}
import java.util.Properties

class AkkaConsumer(props:AkkaConsumerProps) {

  def kafkaConsumerProps(zkConnect:String, groupId:String) = props(Map(
    "zookeeper.connect" -> zkConnect,
    "zookeeper.connection.timeout.ms" -> "10000",
    "group.id" -> groupId,
    "auto.commit.enable" -> "false",
    "zookeeper.session.timeout.ms" -> "1000",
    "zookeeper.sync.time.ms" -> "1000",
    "consumer.timeout.ms" -> "500"
  ))

  def kafkaConsumer(zkConnect:String, groupId:String) = {
    Consumer.create(new ConsumerConfig(kafkaConsumerProps(zkConnect, groupId)))
  }

  def props(map: Map[String, String]): Properties = {
    map.foldLeft(new Properties()) {
      case (p, (k, v)) =>
        p.setProperty(k, v)
        p
    }
  }

  def createConnection(props:AkkaConsumerProps) =  {
    import props._
    val consumerConfig = new ConsumerConfig(kafkaConsumerProps(zkConnect, group))
    val consumerConnector = Consumer.create(consumerConfig)
    system.actorOf(Props(new ConnectorFSM(props, consumerConnector)), "connectorFSM")
  }

  def start(){
    val connector = createConnection(props)
    connector ! ConnectorFSM.Start
  }

}

object AkkaConsumer{
  type Key = Array[Byte]
  type Msg = Array[Byte]
  type MsgStream = KafkaStream[Key, Msg]
}

case class AkkaConsumerProps(system:ActorSystem, zkConnect:String, topic:String, group:String, streams:Int, receiver: ActorRef, maxInFlightPerStream:Int = 64, commitInterval:FiniteDuration = 10 seconds, commitAfterMsgCount:Int = 10000)