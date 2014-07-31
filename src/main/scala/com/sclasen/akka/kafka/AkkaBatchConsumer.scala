package com.sclasen.akka.kafka

import akka.actor._
import akka.pattern._
import akka.util.Timeout
import collection.JavaConverters._
import concurrent.Future
import concurrent.duration._
import java.util.Properties
import kafka.consumer.{TopicFilter, ConsumerConfig, Consumer}
import kafka.serializer.Decoder
import kafka.message.MessageAndMetadata


class AkkaBatchConsumer[Key,Msg,Out](props:AkkaBatchConsumerProps[Key,Msg,Out]) {

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

  def createConnection(props:AkkaBatchConsumerProps[Key,Msg,Out]) =  {
    import props._
    val consumerConfig = new ConsumerConfig(kafkaConsumerProps(zkConnect, group))
    val consumerConnector = Consumer.create(consumerConfig)
    props.connectorActorName.map{
      name =>  system.actorOf(Props(new BatchConnectorFSM(props, consumerConnector)), name)
    }.getOrElse(system.actorOf(Props(new BatchConnectorFSM(props, consumerConnector))))
  }

  def start():Future[Unit] = {
    import props.system.dispatcher
    (connector ? BatchConnectorFSM.Start)(props.startTimeout).map{
      started =>
        props.system.log.info("at=consumer-started")
    }
  }

  def stop():Future[Unit] = {
    import props.system.dispatcher
    (connector ? BatchConnectorFSM.Stop)(props.startTimeout).map{
      stopped =>
        props.system.log.info("at=consumer-stopped")
    }
  }

}

object AkkaBatchConsumerProps{
  def forSystem[Key, Msg, Out](system: ActorSystem,
                          zkConnect: String,
                          topic: String,
                          group: String,
                          streams: Int,
                          keyDecoder: Decoder[Key],
                          msgDecoder: Decoder[Msg],
                          receiver: ActorRef,
                          msgHandler: (MessageAndMetadata[Key,Msg]) => Out = defaultHandler[Key, Msg],
                          connectorActorName:Option[String] = None,
                          batchSize: Int = 1000,
                          batchTimeout: Timeout = Timeout(1 seconds),
                          startTimeout: Timeout = Timeout(5 seconds)): AkkaBatchConsumerProps[Key, Msg, Out] =
    AkkaBatchConsumerProps(system, system, zkConnect, Right(topic), group, streams, keyDecoder, msgDecoder, msgHandler, receiver, connectorActorName, batchSize, batchTimeout, startTimeout)

  def forSystemWithFilter[Key, Msg, Out](system: ActorSystem,
                                    zkConnect: String,
                                    topicFilter: TopicFilter,
                                    group: String,
                                    streams: Int,
                                    keyDecoder: Decoder[Key],
                                    msgDecoder: Decoder[Msg],
                                    receiver: ActorRef,
                                    msgHandler: (MessageAndMetadata[Key,Msg]) => Out = defaultHandler[Key, Msg],
                                    connectorActorName:Option[String] = None,
                                    batchSize: Int = 1000,
                                    batchTimeout: Timeout = Timeout(1 seconds),
                                    startTimeout: Timeout = Timeout(5 seconds)): AkkaBatchConsumerProps[Key, Msg, Out] =
    AkkaBatchConsumerProps(system, system, zkConnect, Left(topicFilter), group, streams, keyDecoder, msgDecoder, msgHandler, receiver, connectorActorName, batchSize, batchTimeout, startTimeout)


  def forContext[Key, Msg, Out](context: ActorContext,
                           zkConnect: String,
                           topic: String,
                           group: String,
                           streams: Int,
                           keyDecoder: Decoder[Key],
                           msgDecoder: Decoder[Msg],
                           receiver: ActorRef,
                           msgHandler: (MessageAndMetadata[Key,Msg]) => Out = defaultHandler[Key, Msg],
                           connectorActorName:Option[String] = None,
                           batchSize: Int = 1000,
                           batchTimeout: Timeout = Timeout(1 seconds),
                           startTimeout: Timeout = Timeout(5 seconds)): AkkaBatchConsumerProps[Key, Msg, Out] =
    AkkaBatchConsumerProps(context.system, context, zkConnect, Right(topic), group, streams, keyDecoder, msgDecoder, msgHandler, receiver, connectorActorName, batchSize, batchTimeout, startTimeout)

  def forContextWithFilter[Key, Msg, Out](context: ActorContext,
                                     zkConnect: String,
                                     topicFilter: TopicFilter,
                                     group: String,
                                     streams: Int,
                                     keyDecoder: Decoder[Key],
                                     msgDecoder: Decoder[Msg],
                                     receiver: ActorRef,
                                     msgHandler: (MessageAndMetadata[Key,Msg]) => Out = defaultHandler[Key, Msg],
                                     connectorActorName:Option[String] = None,
                                     batchSize: Int = 1000,
                                     batchTimeout: Timeout = Timeout(1 seconds),
                                     startTimeout: Timeout = Timeout(5 seconds)): AkkaBatchConsumerProps[Key, Msg, Out] =
    AkkaBatchConsumerProps(context.system, context, zkConnect, Left(topicFilter), group, streams, keyDecoder, msgDecoder, msgHandler, receiver, connectorActorName, batchSize, batchTimeout, startTimeout)

  def defaultHandler[Key,Msg]: (MessageAndMetadata[Key,Msg]) => Msg = msg => msg.message()
}

case class AkkaBatchConsumerProps[Key,Msg,Out](system:ActorSystem,
                                      actorRefFactory:ActorRefFactory,
                                      zkConnect:String,
                                      topicFilterOrTopic:Either[TopicFilter,String],
                                      group:String,
                                      streams:Int,
                                      keyDecoder:Decoder[Key],
                                      msgDecoder:Decoder[Msg],
                                      msgHandler: (MessageAndMetadata[Key,Msg]) => Out,
                                      receiver: ActorRef,
                                      connectorActorName:Option[String],
                                      batchSize:Int = 1000,
                                      batchTimeout:Timeout = Timeout(1 second),
                                      startTimeout:Timeout = Timeout(5 seconds))


