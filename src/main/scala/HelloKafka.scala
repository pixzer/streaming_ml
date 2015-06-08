/**
 * Created by snudurupati on 6/3/15.
 * A simple test implementation of spark-kafka connectivity.
 */
import kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import com.datastax.spark.connector.streaming._

object HelloKafka {

  def main (args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);

    val brokers = "localhost:9092"
    val topics = "sparkml"

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("HelloKafka").setMaster("local")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "auto.offset.reset" -> "smallest")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    //Get lines from the stream and print them.
    messages.foreachRDD(r => r.foreach(println))
    //Start the streaming context
    ssc.start()
    ssc.awaitTermination()
  }

}
