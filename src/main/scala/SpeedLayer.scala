/**
 * Created by snudurupati on 6/4/15.
 * Batch layer of a lambda architecture where append-only data is collected onto cassandra.
 */

import java.sql.Timestamp

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
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.StreamingLinearRegressionWithSGD
import org.apache.spark.mllib.linalg.Vectors
import java.sql.Timestamp
import java.util.Date

object SpeedLayer {

  //case class HosuingData(CRIM: Double, ZN: Double, INDUS: Double, CHAS: Double, NOX: Double, RM: Double, AGE: Double, DIS: Double, RAD: Double, TAX: Double, PTRATIO: Double, B: Double, LSTAT: Double, MEDV: Double)

  def main (args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val brokers = "localhost:9092"
    val topics = "sparkml"

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("BatchLayer")
      .setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val cass = CassandraConnector(sparkConf)

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "auto.offset.reset" -> "smallest")
    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    //Create a DStream of LabeledPoints from the incoming stream

    val labeledStream = stream.map {
        case (k, v) => val instance = v.split(";")
        val y = instance(instance.length - 1).toDouble
        val features = instance.slice(0, instance.length - 2).map(_.toDouble)
        LabeledPoint(y, Vectors.dense(features))
    }

    //labeledStream.print()
    val featureVector = stream.map {
        case (k, v) => val instance = v.split(";")
        val features = instance.slice(0, instance.length - 2).map(_.toDouble)
        Vectors.dense(features)
    }
    //featureVector.print()

    //Initialize the LinearRegression model with numIterations, stepSize and initial zero weights.
    val numFeatures = 12

    val stepSize = 0.00001
    val zeroVector = Vectors.zeros(numFeatures)
    val model = new StreamingLinearRegressionWithSGD()
      .setInitialWeights(zeroVector)
      .setNumIterations(10)
      .setStepSize(stepSize)

    //train the model incrementally
    model.trainOn(labeledStream)
    val predictions = model.predictOnValues(labeledStream.map(lp => (lp.label, lp.features)))


    //Create a DStream of Predictions and labels to be used for error calculation.
    val predsAndlabels = labeledStream.transform { (rdd, time) =>
      val latest = model.latestModel()
      rdd.map{
        val today = new Date()
        val timstmp = new Timestamp(today.getTime)
        p => val pred = latest.predict(p.features)
          (timstmp, p.label, pred)
      }

    }

    predsAndlabels.print()

    //Store predictions and labels onto Cassandra
    predsAndlabels.saveToCassandra("sparkml", "predictions")

    //calculate MSE and RMSE for the model
    val errorRates = predsAndlabels.foreachRDD { rdd =>
      val mse = rdd.map{case(t, v, p) => math.pow((v - p), 2)}.mean()
      val rmse = math.sqrt(mse)
      println("MSE and RMSE respectively for each batch are: %2.4f, %2.4f".format(mse, rmse))
      cass.withSessionDo(session => session.execute("insert into sparkml.accuracy (event_time, mse, rmse) values (now(), " + mse + "," + rmse + ")"))
        //println("insert into sparkml.accuracy (event_time, mse, rmse) values (now(), "+ mse + "," + rmse + ")")
    }

    //Start the streaming context
    ssc.start()
    //println(model)
    ssc.awaitTermination()
  }

}
