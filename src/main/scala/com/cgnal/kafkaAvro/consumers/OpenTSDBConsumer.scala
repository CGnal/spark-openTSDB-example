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
class OpenTSDBConsumer{

  val logger = LoggerFactory.getLogger(this.getClass)
  var openTSDBContext: Option[OpenTSDBContext] = None

  //val hadoopConf: Configuration = HBaseConfiguration.create()
  //hadoopConf.set("hbase.master", "127.0.0.1:60000")
  //hadoopConf.set("hbase.zookeeper.quorum", "127.0.0.1")
  //hadoopConf.set("hbase.zookeeper.property.clientPort", "2181")


  def run(implicit ssc: StreamingContext, hadoopConf: Configuration, topicsSet: Set[String], kafkaParams: Map[String, String]): Unit = {

    logger.info("Consumer is running")
    val sparkContext = ssc.sparkContext
    val sqlContext = new SQLContext(sparkContext)
    openTSDBContext = Some(new OpenTSDBContext(sqlContext, Some(hadoopConf)))
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
