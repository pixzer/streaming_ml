/**
 * Created by snudurupati on 6/9/15.
 * Serving layer of a lambda architecture using data stored by batch and speed laters onto Cassandra using SparkSQL + Dataframes.
 */
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.SomeColumns

object ServingLayer {

  def main (args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);

    val brokers = "localhost:9092"
    val topics = "sparkml"

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("HelloKafka").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    //create a dataframe from a Cassandra table
    val predictions = sqlContext.load("org.apache.spark.sql.cassandra",
      options = Map("c_table" -> "predictions", "keyspace" -> "sparkml"))

    //collect all rows of the Dataframe
    predictions.sort("event_time").collect().foreach(println)
  }

}