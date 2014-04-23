package com.sclasen.akka.kafka

import akka.actor.{Props, ActorRef, ActorSystem}
import akka.pattern._
import akka.util.Timeout
import collection.JavaConverters._
import concurrent.Future
import concurrent.duration._
import java.util.Properties
import kafka.consumer.{ConsumerConfig, Consumer}
import kafka.serializer.Decoder

object AkkaConsumer{
  def toProps(props:collection.mutable.Set[(String,String)]): Properties = {
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

  def kafkaConsumerProps(zkConnect:String, groupId:String) = {
    val consumerConfig = props.system.settings.config.getConfig("kafka.consumer")
    val consumerProps = consumerConfig.entrySet().asScala.map{
      entry => entry.getKey -> consumerConfig.getString(entry.getKey)
    } ++ Set("zookeeper.connect" -> zkConnect, "group.id" -> groupId)
    toProps(consumerProps)
  }

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
        props.system.log.info("at=consumer-started")
    }
  }

  def commit():Future[Unit] = {
    import props.system.dispatcher
    (connector ? ConnectorFSM.Commit)(props.commitTimeout).map{
      committed =>
        props.system.log.info("at=consumer-committed")
    }
  }
}

case class AkkaConsumerProps[Key,Msg](system:ActorSystem,
                                      zkConnect:String,
                                      topic:String,
                                      group:String,
                                      streams:Int,
                                      keyDecoder:Decoder[Key],
                                      msgDecoder:Decoder[Msg],
                                      receiver: ActorRef,
                                      maxInFlightPerStream:Int = 64,
                                      commitInterval:FiniteDuration = 10 seconds,
                                      commitAfterMsgCount:Int = 10000,
                                      startTimeout:Timeout = Timeout(5 seconds),
                                      commitTimeout:Timeout = Timeout(5 seconds)
                                       )