organization := "com.sclasen"

name := "akka-kafka"

version := "0.0.9-SNAPSHOT"

scalaVersion := "2.10.4"

parallelExecution in Test := false

//resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

resolvers += "spray repo" at "http://repo.spray.io"


libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.3.2" % "compile"

libraryDependencies += "com.typesafe.akka" %% "akka-slf4j" % "2.3.2" % "test,it"

libraryDependencies +=   "org.apache.kafka" %% "kafka"  %"0.8.1" exclude("log4j", "log4j") exclude("org.slf4j","slf4j-log4j12")

libraryDependencies += "com.typesafe.akka" %% "akka-testkit" % "2.3.2" % "test,it"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.0" % "test,it"

libraryDependencies += "commons-io" % "commons-io" % "2.4" % "test,it"

libraryDependencies += "org.slf4j" % "log4j-over-slf4j" % "1.6.6" % "provided"

parallelExecution in Test := false

pomExtra := (
  <url>http://github.com/sclasen/akka-kafka</url>
    <licenses>
      <license>
        <name>The Apache Software License, Version 2.0</name>
        <url>http://www.apache.org/licenses/LICENSE-2.0.html</url>
        <distribution>repo</distribution>
      </license>
    </licenses>
    <scm>
      <url>git@github.com:sclasen/akka-kafka.git</url>
      <connection>scm:git:git@github.com:sclasen/akka-kafka.git</connection>
    </scm>
    <developers>
      <developer>
        <id>sclasen</id>
        <name>Scott Clasen</name>
        <url>http://github.com/sclasen</url>
      </developer>
    </developers>)


publishTo <<= version {
  (v: String) =>
    val nexus = "https://oss.sonatype.org/"
    if (v.trim.endsWith("SNAPSHOT")) Some("snapshots" at nexus + "content/repositories/snapshots")
    else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}


val root = Project("akka-kafka", file(".")).configs(IntegrationTest).settings(Defaults.itSettings:_*)
