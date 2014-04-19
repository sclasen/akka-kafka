package com.sclasen.akka.kafka

import akka.actor.{Props, ActorRef, ActorSystem}
import akka.pattern._
import scala.concurrent.duration._
import kafka.consumer.{ConsumerConfig, Consumer}
import java.util.Properties
import kafka.serializer.Decoder
import scala.concurrent.Future
import akka.util.Timeout

object AkkaConsumer{
  def toProps(props:(String, String)*): Properties = {
    props.foldLeft(new Properties()) {
      case (p, (k, v)) =>
        p.setProperty(k, v)
        p
    }
  }
}

class AkkaConsumer[Key,Msg](props:AkkaConsumerProps[Key,Msg]) {

  import AkkaConsumer._

  lazy val connector = createConnection(props)

  def kafkaConsumerProps(zkConnect:String, groupId:String) = toProps(
    "zookeeper.connect" -> zkConnect,
    "zookeeper.connection.timeout.ms" -> "10000",
    "group.id" -> groupId,
    "auto.commit.enable" -> "false",
    "zookeeper.session.timeout.ms" -> "1000",
    "zookeeper.sync.time.ms" -> "1000",
    "consumer.timeout.ms" -> "200"
  )

  def kafkaConsumer(zkConnect:String, groupId:String) = {
    Consumer.create(new ConsumerConfig(kafkaConsumerProps(zkConnect, groupId)))
  }

  def createConnection(props:AkkaConsumerProps[Key,Msg]) =  {
    import props._
    val consumerConfig = new ConsumerConfig(kafkaConsumerProps(zkConnect, group))
    val consumerConnector = Consumer.create(consumerConfig)
    system.actorOf(Props(new ConnectorFSM(props, consumerConnector)), "connectorFSM")
  }

  def start():Future[Unit] = {
    import props.system.dispatcher
    (connector ? ConnectorFSM.Start)(props.startTimeout).map{
      started =>
        props.system.log.info("consumer started")
    }
  }

  def commit():Future[Unit] = {
    import props.system.dispatcher
    (connector ? ConnectorFSM.Commit)(props.commitTimeout).map{
      started =>
        props.system.log.info("consumer committed")
    }
  }
}

case class AkkaConsumerProps[Key,Msg](system:ActorSystem, zkConnect:String, topic:String, group:String, streams:Int,
                                      keyDecoder:Decoder[Key], msgDecoder:Decoder[Msg],
                                      receiver: ActorRef, maxInFlightPerStream:Int = 64,
                                      commitInterval:FiniteDuration = 10 seconds, commitAfterMsgCount:Int = 10000,
                                      startTimeout:Timeout = Timeout(5 seconds), commitTimeout:Timeout = Timeout(5 seconds))