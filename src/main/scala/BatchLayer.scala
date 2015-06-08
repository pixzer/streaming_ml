/**
 * Created by snudurupati on 6/3/15.
 * Batch layer of a lambda architecture where append-only data is collected onto cassandra.
 */
import kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.SomeColumns

object BatchLayer {

  def main (args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);

    val brokers = "localhost:9092"
    val topics = "sparkml"

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("BatchLayer")
      .setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "auto.offset.reset" -> "smallest")
    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    //Create a new Cassandra keyspace and tables and truncate the table if already existing
    /*val cass = CassandraConnector(sparkConf)
    cass.withSessionDo { session =>
      session.execute(s"CREATE KEYSPACE IF NOT EXISTS batch_layer WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
      session.execute(s"CREATE TABLE IF NOT EXISTS batch_layer.spark_ml (key VARCHAR PRIMARY KEY, value INT)")
      session.execute(s"TRUNCATE batch_layer.spark_ml")
    }*/

    //Get lines from the stream and store onto Cassandra
    stream.map { case (_, v) => v }
      .map(x => (x, 1))
      .reduceByKey(_ + _)
      .saveToCassandra("batch_layer", "spark_ml", SomeColumns("key", "value"))


    //Start the streaming context
    ssc.start()
    ssc.awaitTermination()
  }

}
