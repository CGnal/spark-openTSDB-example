package com.cgnal.kafkaAvro.consumers
import java.io.ByteArrayInputStream

import com.cgnal.DataPoint
import com.cgnal.spark.opentsdb.OpenTSDBContext
import com.gensler.scalavro.io.AvroTypeIO
import com.gensler.scalavro.types.AvroType
import kafka.serializer.DefaultDecoder
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.slf4j.LoggerFactory

import scala.util.Success

/**
  * Created by cgnal on 20/09/16.
  */
class OpenTSDBConsumer(ssc: StreamingContext, hadoopConf: Configuration){

  val logger = LoggerFactory.getLogger(this.getClass)
  var openTSDBContext: Option[OpenTSDBContext] = initOpenTSDB()

  private def initOpenTSDB() = {
    val sparkContext = ssc.sparkContext
    val sqlContext = new SQLContext(sparkContext)
    Some(new OpenTSDBContext(sqlContext, Some(hadoopConf)))
  }

  def this (ssc: StreamingContext, hadoopConf: Configuration, keytab:String, principal:String) = {
    this(ssc, hadoopConf)
    openTSDBContext.get.keytab = keytab
    openTSDBContext.get.principal = principal
    openTSDBContext.get.keytabLocalTempDir = "/tmp"
  }

  def run(topicsSet: Set[String], kafkaParams: Map[String, String]): Unit = {

    assert(kafkaParams.contains("metadata.broker.list"))

    val inputStream: InputDStream[(Array[Byte], Array[Byte])] = KafkaUtils.createDirectStream[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder](ssc, kafkaParams, topicsSet)

    val stream = inputStream.map { el =>
      val schema = AvroType[DataPoint]
      val bis = new ByteArrayInputStream(el._2)
      val io: AvroTypeIO[DataPoint] = schema.io
      val Success(dataPoint) = io.read(bis)
      dataPoint
    }.map{dt =>
      //print(dt)
      com.cgnal.spark.opentsdb.DataPoint[Double](dt.metric, dt.timestamp, dt.value, dt.tags)}
    stream.print(10)
    openTSDBContext.get.streamWrite(stream)
  }

}
