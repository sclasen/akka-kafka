
akka-kafka
==========

[![Build Status](https://travis-ci.org/sclasen/akka-kafka.svg?branch=master)](https://travis-ci.org/sclasen/akka-kafka)

Actor based kafka consumer built on top of the high level kafka consumer.

The `0.0.X` versions of akka-kafka were built with kafka `0.8.1.1`. Starting with `0.1.0` akka-kafka is built against the scala 2.11 version of kafka `0.8.2.0`

AkkaConsumer
------------

Manages backpressure so the consumer doesn't overwhelm other parts of the system.
The consumer allows asynchronous/concurrent processing of a configurable, bounded number of in-flight messages.

You can configure the connector to commit offsets at a configurable interval, after a configured number of messages are processed, or simply commit the offsets programatically.
It waits until all in flight messages are processed at commit time, so you know that everything that's committed has been processed.

AkkaBatchConsumer
-----------------

Configured and used very similarly to the `AkkaConsumer` but it accumulates a batch of messages, with a configured maximum size and timeout, and sends
the batch to your `ActorRef`, you reply with `BatchConnectorFSM.BatchProcessed` and the batch consumer will commit the batch and accumulate a new one.

use
===

Add the dependencies to your project. akka-kafka by default excludes kafka's logging dependencies, in favor of slf4j, but
does so without forcing you to do the same, so you need to add a dependency that brings in log4j or slf4j's log4j.

If you dont want to use slf4j, you can substitute log4j.

```scala
/* build.sbt */

libraryDependencies += "com.sclasen" %% "akka-kafka" % "0.1.0" % "compile"

libraryDependencies += "com.typesafe.akka" %% "akka-slf4j" % "2.3.9" % "compile"

libraryDependencies += "org.slf4j" % "log4j-over-slf4j" % "1.7.10" % "compile"
```

AkkaConsumer
------------

To use this consumer you must provide it with an actorRef that will receive messages from kafka, and will reply to the sender
with `StreamFSM.Processed` after a message has been successfully processed.  If you do not reply in this way to every single message received, the connector will not be able
to drain all in-flight messages at commit time, and will hang.

Note that the sender in this case is not the `ConnectorFSM` actor, but one of the `StreamFSM` actors that are the connector's children. You need to reply to
the `sender()`, a `StreamFSM` rather than the connector itself so each stream can track the number of outstanding messages correctly.

Here is an example of such an actor that just prints the messages.

```scala
class Printer extends Actor{

  def receive = {
    case x:Any =>
      println(x)
      sender ! StreamFSM.Processed
  }

}
```

The consumer is configured with an instance of `AkkaConsumerProps`, which looks like this.

```scala
case class AkkaConsumerProps[Key,Msg](system:ActorSystem,
                                      actorRefFactory:ActorRefFactory,
                                      connectorActorName:Option[String],
                                      zkConnect:String,
                                      topicFilterOrTopic:Either[TopicFilter, String],
                                      group:String,
                                      streams:Int,
                                      keyDecoder:Decoder[Key],
                                      msgDecoder:Decoder[Msg],
                                      msgHandler: (MessageAndMetadata[Key,Msg]) => Any,
                                      receiver: ActorRef,
                                      maxInFlightPerStream:Int = 64,
                                      startTimeout:Timeout = Timeout(5 seconds),
                                      commitConfig:CommitConfig = CommitConfig())

case class CommitConfig(commitInterval:Option[FiniteDuration] = Some(10 seconds),
                        commitAfterMsgCount:Option[Int] = Some(10000),
                        commitTimeout:Timeout = Timeout(5 seconds))
```

there are 4 helper methods on the `AkkaConsumerProps object` that ease the construction of  `AkkaConsumerProps`.

Use `AkkaConsumerProps.forSystem(system = ...)` when you only need a single connector in your application. The connector will be created as a top level actor.

Use `AkkaConsumerProps.forContext(context = ...)` when you want to have multiple connectors in your application, or you want the connector to be a child of one of your actors.

Use `AkkaConsumerProps.forSystemWithFilter(system = ...)` for using a TopicFilter, when you only need a single connector in your application. The connector will be created as a top level actor.

Use `AkkaConsumerProps.forContextWithFilter(context = ...)` for using a TopicFilter, when you want to have multiple connectors in your application, or you want the connector to be a child of one of your actors.


So a full example of getting a consumer up and running looks like this.

```scala
import akka.actor.{Props, ActorSystem, Actor}
import com.sclasen.akka.kafka.{AkkaConsumer, AkkaConsumerProps, StreamFSM}
import kafka.serializer.DefaultDecoder

object Example {
  class Printer extends Actor{
    def receive = {
      case x:Any =>
        println(x)
        sender ! StreamFSM.Processed
    }
  }

  val system = ActorSystem("test")
  val printer = system.actorOf(Props[Printer])


  /*
  the consumer will have 4 streams and max 64 messages per stream in flight, for a total of 256
  concurrently processed messages.
  */
  val consumerProps = AkkaConsumerProps.forSystem(
    system = system,
    zkConnect = "localhost:2181",
    topic = "your-kafka-topic",
    group = "your-consumer-group",
    streams = 4, //one per partition
    keyDecoder = new DefaultDecoder(),
    msgDecoder = new DefaultDecoder(),
    receiver = printer
  )

  val consumer = new AkkaConsumer(consumerProps)

  consumer.start()  //returns a Future[Unit] that completes when the connector is started

  consumer.commit() //returns a Future[Unit] that completes when all in-flight messages are processed and offsets are committed.

  consumer.stop()   //returns a Future[Unit] that completes when the connector is stopped.

}
```

configure
=========

The `reference.conf` file is used to provide defaults for the properties used to configure the underlying kafka consumer.

To override any of these properties, use the standard akka `application.conf` mechanism.

Note that the property values must be strings, as that is how kafka expects them.

Do NOT set the `consumer.timeout.ms` to `-1`. This will break everything. Don't set `auto.commit.enable` either.

```
kafka.consumer {
    zookeeper.connection.timeout.ms = "10000"
    auto.commit.enable = "false"
    zookeeper.session.timeout.ms = "1000"
    zookeeper.sync.time.ms =  "1000"
    consumer.timeout.ms =  "400"
}
```


AkkaBatchConsumer
-----------------

To use this consumer you must provide it with an actorRef that will receive a Batch of messages from kafka, and will reply to the sender
with `BatchConnectorFSM.BatchProcessed` after a batch has been successfully processed.  If you do not reply in this way to every single batch received, the connector will not be able
to commit the batch, and will hang. 

`AkkaBatchConsumerProps` has similar convenience methods to `AkkaConsumerProps`

So a full example of getting a batch consumer up and running looks like this.

```scala
import akka.actor.{Props, ActorSystem, Actor}
import com.sclasen.akka.kafka.{AkkaBatchConsumer, AkkaBatchConsumerProps, BatchConnectorFSM}
import kafka.serializer.DefaultDecoder

//object BatchExample {
  class BatchPrinter extends Actor{
    def receive = {
      case MyBatch(xs) =>
        xs.foreach(println)
        sender ! BatchConnectorFSM.BatchProcessed
    }
  }

  type B = Array[Byte]

  case class MyBatch(msgs: IndexedSeq[String])

  /*provide a function that makes your batch object from an IndexedSeq of messages, if desired.*/
  def makeBatch(msgs: IndexedSeq[String]):MyBatch = MyBatch(msgs)

  /*provide a function that transforms the kafka message payload, if desired.*/
  def messageHandler(msg: Array[Byte]):String = new String(msg)

  val system = ActorSystem("batchTest")
  val printer = system.actorOf(Props[BatchPrinter])


  /*
  the consumer will have 4 streams and accumulate a batch of up to 1000 messages before sending the batch.
  if no message is received for 1 second, the partial batch is sent instead.
  */
  val consumerProps = AkkaBatchConsumerProps.forSystem[B,B,String,MyBatch](
    system = system,
    zkConnect = "localhost:2181",
    topic = "your-kafka-topic",
    group = "your-consumer-group",
    streams = 4, //one per partition
    keyDecoder = new DefaultDecoder(),
    msgDecoder = new DefaultDecoder(),
    msgHandler = messageHandler(_),
    batchHandler = makeBatch(_),
    receiver = printer
  )

  val consumer = new AkkaBatchConsumer(consumerProps)

  consumer.start()  //returns a Future[Unit] that completes when the connector is started

  consumer.stop()   //returns a Future[Unit] that completes when the connector is stopped.

}
```

develop
=======

Assumes you have forego installed. if not, `go get github.com/ddollar/forego`

```
./bin/setup         #installs kafka to ./kafka-install
cp .env.sample .env #used by forego to set your env vars
forego start        #starts zookeeper and kafka
```
In another shell

```
forego run sbt
```

