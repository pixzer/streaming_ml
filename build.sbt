name := "streaming_ml"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.3.0",
  "org.apache.spark" %% "spark-sql" % "1.3.0",
  "org.apache.spark" %% "spark-mllib" % "1.3.0",
  "org.apache.spark" %% "spark-streaming-kafka" % "1.3.0",
  "com.datastax.spark" %% "spark-cassandra-connector" % "1.3.0-M1",
  "org.apache.commons" % "commons-csv" % "1.1"
)

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"