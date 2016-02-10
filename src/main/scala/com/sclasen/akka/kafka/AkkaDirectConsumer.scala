package com.sclasen.akka.kafka

import java.util.concurrent.{TimeUnit, Executors}
import akka.actor._
import akka.util.Timeout
import collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import concurrent.duration._
import kafka.consumer._
import kafka.serializer.Decoder
import kafka.message.MessageAndMetadata

class AkkaDirectConsumer[Key,Msg](props:AkkaDirectConsumerProps[Key,Msg]) {

  import AkkaConsumer._

  lazy val connector = createConnection(props)
  lazy implicit val ecForBlockingIterator = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(props.streams))

  def kafkaConsumerProps(zkConnect:String, groupId:String) = {
    val consumerConfig = props.system.settings.config.getConfig("kafka.consumer")
    val required = Set("zookeeper.connect" -> zkConnect, "group.id" -> groupId, "consumer.timeout.ms" -> "-1", "auto.commit.enable" -> "true")
    val requiredKeys = required.map(_._1)
    val consumerProps = consumerConfig.entrySet().asScala
      .filter( k => !requiredKeys.contains(k.getKey) )
      .map{
        entry => entry.getKey -> consumerConfig.getString(entry.getKey)
      } ++ required
    toProps(consumerProps)
  }

  def kafkaConsumer(zkConnect:String, groupId:String) = {
    Consumer.create(new ConsumerConfig(kafkaConsumerProps(zkConnect, groupId)))
  }

  def createConnection(props:AkkaDirectConsumerProps[Key,Msg]) =  {
    import props._
    val consumerConfig = new ConsumerConfig(kafkaConsumerProps(zkConnect, group))
    Consumer.create(consumerConfig)
  }

  def createStream = {
    props.topicFilterOrTopic match {
      case Left(t) =>
        props.system.log.info(s"createStream for topic: ${t}")
        connector.createMessageStreamsByFilter(t, props.streams, props.keyDecoder, props.msgDecoder)
      case Right(t) =>
        props.system.log.info(s"createStream for topic: ${t}")
        connector.createMessageStreams(Map(t -> props.streams), props.keyDecoder, props.msgDecoder).apply(t)
    }
  }

  def start():Future[Unit] = {
    val streams = createStream
    val f = streams.map { stream =>
      val it = stream.iterator()
      def hasNext = try {
        it.hasNext()
      } catch {
        case cte: ConsumerTimeoutException =>
          props.system.log.warning("AkkaHighLevelConsumer should not see ConsumerTimeoutException")
          false
      }
      Future {
        props.system.log.debug("blocking on stream")
        while (hasNext) {
          val msg = props.msgHandler(it.next())
          props.receiver ! msg
        }
      }(ecForBlockingIterator) // or mark the execution context implicit. I like to mention it explicitly.
    }
    Future.sequence(f).map{_ => Unit}
  }

  def stop():Future[Unit] = {
    connector.shutdown()
    ecForBlockingIterator.shutdown()
    try {
      if (!ecForBlockingIterator.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
        props.system.log.warning("Timed out waiting for consumer threads to shut down, exiting uncleanly");
      }
    } catch {
      case e:InterruptedException =>
        props.system.log.warning("Interrupted during shutdown, exiting uncleanly");
    }
    Future.successful(Unit)
  }
}

object AkkaDirectConsumerProps {
  def forSystem[Key, Msg](system: ActorSystem,
                          zkConnect: String,
                          topic: String,
                          group: String,
                          streams: Int,
                          keyDecoder: Decoder[Key],
                          msgDecoder: Decoder[Msg],
                          receiver: ActorRef,
                          msgHandler: (MessageAndMetadata[Key,Msg]) => Any = defaultHandler[Key, Msg],
                          connectorActorName:Option[String] = None,
                          startTimeout: Timeout = Timeout(5 seconds)): AkkaDirectConsumerProps[Key, Msg] =
    AkkaDirectConsumerProps(system, system, zkConnect, Right(topic), group, streams, keyDecoder, msgDecoder, msgHandler, receiver, connectorActorName, startTimeout)

  def forSystemWithFilter[Key, Msg](system: ActorSystem,
                                    zkConnect: String,
                                    topicFilter: TopicFilter,
                                    group: String,
                                    streams: Int,
                                    keyDecoder: Decoder[Key],
                                    msgDecoder: Decoder[Msg],
                                    receiver: ActorRef,
                                    msgHandler: (MessageAndMetadata[Key,Msg]) => Any = defaultHandler[Key, Msg],
                                    connectorActorName:Option[String] = None,
                                    startTimeout: Timeout = Timeout(5 seconds)): AkkaDirectConsumerProps[Key, Msg] =
    AkkaDirectConsumerProps(system, system, zkConnect, Left(topicFilter), group, streams, keyDecoder, msgDecoder, msgHandler, receiver, connectorActorName, startTimeout)


  def forContext[Key, Msg](context: ActorContext,
                           zkConnect: String,
                           topic: String,
                           group: String,
                           streams: Int,
                           keyDecoder: Decoder[Key],
                           msgDecoder: Decoder[Msg],
                           receiver: ActorRef,
                           msgHandler: (MessageAndMetadata[Key,Msg]) => Any = defaultHandler[Key, Msg],
                           connectorActorName:Option[String] = None,
                           startTimeout: Timeout = Timeout(5 seconds)): AkkaDirectConsumerProps[Key, Msg] =
    AkkaDirectConsumerProps(context.system, context, zkConnect, Right(topic), group, streams, keyDecoder, msgDecoder, msgHandler, receiver,connectorActorName, startTimeout)

  def forContextWithFilter[Key, Msg](context: ActorContext,
                                     zkConnect: String,
                                     topicFilter: TopicFilter,
                                     group: String,
                                     streams: Int,
                                     keyDecoder: Decoder[Key],
                                     msgDecoder: Decoder[Msg],
                                     receiver: ActorRef,
                                     msgHandler: (MessageAndMetadata[Key,Msg]) => Any = defaultHandler[Key, Msg],
                                     connectorActorName:Option[String] = None,
                                     startTimeout: Timeout = Timeout(5 seconds)): AkkaDirectConsumerProps[Key, Msg] =
    AkkaDirectConsumerProps(context.system, context, zkConnect, Left(topicFilter), group, streams, keyDecoder, msgDecoder, msgHandler, receiver,connectorActorName, startTimeout)

  def defaultHandler[Key,Msg]: (MessageAndMetadata[Key,Msg]) => Any = msg => msg.message()
}

case class AkkaDirectConsumerProps[Key,Msg](system:ActorSystem,
                                               actorRefFactory:ActorRefFactory,
                                               zkConnect:String,
                                               topicFilterOrTopic:Either[TopicFilter,String],
                                               group:String,
                                               streams:Int,
                                               keyDecoder:Decoder[Key],
                                               msgDecoder:Decoder[Msg],
                                               msgHandler: (MessageAndMetadata[Key,Msg]) => Any,
                                               receiver: ActorRef,
                                               connectorActorName:Option[String],
                                               startTimeout:Timeout = Timeout(5 seconds))
