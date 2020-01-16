
version := "0.1"

//scalaVersio n := "2.13.0"
resolvers += "confluent" at "https://packages.confluent.io/maven/"
//libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.1.0-cp1"
//libraryDependencies += "com.typesafe" % "config" % "1.2.1"
libraryDependencies ++=Seq(
  "com.lightbend.akka" %% "akka-stream-alpakka-file" % "1.1.2",
  "com.lightbend.akka" %% "akka-stream-alpakka-csv" % "1.1.2",
  "com.typesafe.akka" %% "akka-stream-kafka" % "1.1.0",
  "org.apache.kafka" % "kafka-clients" % "2.1.0-cp1",
  "com.typesafe.akka" %% "akka-stream" % "2.5.26",
  "com.typesafe" % "config" % "1.2.1",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
   "org.slf4j" % "slf4j-simple" % "1.7.21",
   "ch.qos.logback" % "logback-classic" % "1.2.3",
  "io.spray" %%  "spray-json" % "1.3.4"
  //"com.typesafe.akka" %% "akka-actor"   % "2.2-M3",
  //"com.typesafe.akka" %% "akka-slf4j"   % "2.2-M3",
  //"com.typesafe.akka" %% "akka-remote"  % "2.2-M3",
  //"com.typesafe.akka" %% "akka-agent"   % "2.2-M3",
  //"com.typesafe.akka" %% "akka-testkit" % "2.2-M3" % "test"
)
//initialCommands := """|import com.typesafe.scalalogging._
//|import org.slf4j.{ Logger => Underlying, _ }""".stripMargin
