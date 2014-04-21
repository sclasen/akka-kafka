akka-kafka
==========

Actor based kafka consumer built on top of the high level kafka consumer.

Manages backpressure so the consumer doesn't overwhelm other parts of the system.

Commits offsets at a configurable interval, and also after a configurable number of messages are processed, or also programatically.

The consumer allows asynchronous/concurrent processing of a configurable bounded number of in-flight messages.

use
===

Add the dependencies to your project. akka-kafka by default excludes kafka's logging dependencies, in favor of slf4j, but
does so without forcing you to do the same, so you need to add a dependency that brings in log4j or slf4j's log4j.

If you dont want to use slf4j, you can substitute log4j.

```scala
/* build.sbt */

libraryDependencies += "com.sclasen" %% "akka-kafka" % "0.0.2" % "compile"

libraryDependencies += "com.typesafe.akka" %% "akka-slf4j" % "2.3.2" % "compile"

libraryDependencies += "org.slf4j" % "log4j-over-slf4j" % "1.6.6" % "compile"
```

To use this library you must provide it with an actorRef that will receive messages from kafka, and will reply to the sender
with `StreamFSM.Processed` after a message has been successfully processed.  If you do not reply in this way to every single message received, the connector will not be able
to drain all in-flight messages at commit time, and will hang.

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
```

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
  val consumerProps = AkkaConsumerProps(
    system = system,
    zkConnect = "localhost:2181",
    topic = "your-kafka-topic",
    group = "your-consumer-group",
    streams = 4, //one per partition
    keyDecoder = new DefaultDecoder(),
    msgDecoder = new DefaultDecoder(),
    receiver = printer,
    maxInFlightPerStream = 64
  )

  val consumer = new AkkaConsumer(consumerProps)

  consumer.start()  //returns a Future[Unit] that completes when the connector is started

  consumer.commit() //returns a Future[Unit] that completes when all in-flight messages are processed and offsets are committed.

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

