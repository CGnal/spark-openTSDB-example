package com.cgnal.kafkaAvro

import java.sql.Timestamp
import java.time.Instant
import java.util.Properties
import java.util.concurrent.Executors

import com.cgnal.kafkaAvro.consumers.OpenTSDBConsumer
import com.cgnal.kafkaAvro.producers.KafkaAvroProducer
import com.cgnal.services.{HbaseLocal, KafkaLocal}
import com.cgnal.spark.opentsdb.DataPoint
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{HTable, Result, ResultScanner, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.varia.NullAppender
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpec}

import scala.collection.mutable

/**
  * Created by cgnal on 19/09/16.
  */
class OpenTSDBConsumerSpec extends WordSpec with MustMatchers with BeforeAndAfterAll {


  Logger.getRootLogger().removeAllAppenders();
  Logger.getRootLogger().addAppender(new NullAppender());

  var hbaseLocal: HbaseLocal = new HbaseLocal
  var kafkaLocal: KafkaLocal = new KafkaLocal(false)
  val topic = "test3"
  val producer = new KafkaAvroProducer()
  var sparkContext: SparkContext = _
  var streamingContext: StreamingContext = _
  var consumer : OpenTSDBConsumer = _
  val metric = ConfigFactory.load().getString("spark-opentsdb-exmaples.openTSDB.metric")

  val propsProducer = new Properties()
  propsProducer.put("bootstrap.servers", "localhost:9092")
  propsProducer.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
  propsProducer.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")

  val propsConsumer = Map(
    "value.deserializer" -> "org.apache.kafka.common.serialization.ByteArrayDeserializer",
    "key.deserializer" -> "org.apache.kafka.common.serialization.ByteArrayDeserializer",
    "bootstrap.servers" -> "localhost:9092",
    "metadata.broker.list" -> "localhost:9092"
  )
  val brokers = ConfigFactory.load().getString("spark-opentsdb-exmaples.kafka.brokers")
  val props = Map("metadata.broker.list" -> brokers)

  val hadoopConf = HBaseConfiguration.create()
  hadoopConf.set("hbase.master", "127.0.0.1:60000")
  hadoopConf.set("hbase.zookeeper.quorum", "127.0.0.1")
  hadoopConf.set("hbase.zookeeper.property.clientPort", "2181")

  val conf = new SparkConf().
    setAppName("spark-opentsdb-example-test").
    setMaster("local[*]")

  override def beforeAll(): Unit = {
    hbaseLocal.start()
    sparkContext = new SparkContext(conf)
    sparkContext.setLogLevel("ERROR")
    streamingContext = new StreamingContext(sparkContext, Milliseconds(500))
    Thread.sleep(10000)
    kafkaLocal.start()
    kafkaLocal.createTopic(topic)
  }

  "OpenTSDBConsumer" must {
    "should capture the stream from kafka and write it on HBASE" in {

      val thread = new Thread(new Runnable {
        override def run(): Unit = producer.run(2, 100, 500, propsProducer, topic)
      })
      thread.start()

      consumer = new OpenTSDBConsumer(streamingContext, hadoopConf)
      consumer.run(Set(topic), props)
      streamingContext.start()
      Thread.sleep(2000)
      streamingContext.stop(false, false)

      val conf = HBaseConfiguration.create()

      val hbaseContext = new HBaseContext(sparkContext, conf)

      val scan = new Scan()
      scan.setCaching(100)

      val getRdd: RDD[(ImmutableBytesWritable, Result)] = hbaseContext.hbaseRDD(TableName.valueOf("tsdb"), scan)

      val df = consumer.openTSDBContext.get.loadDataFrame(metric, Map("tag"-> "hello"), None)

      df.schema must be(
        StructType(
          Array(
            StructField("timestamp", TimestampType, nullable = false),
            StructField("metric", StringType, nullable = false),
            StructField("value", DoubleType, nullable = false),
            StructField("tags", DataTypes.createMapType(StringType, StringType), nullable = false)
          )
        )
      )

      df.collect().length must be equals 200
    }
  }

  override def afterAll(): Unit = {
    kafkaLocal.stop()
    Thread.sleep(5000)
    hbaseLocal.stop()
    //consumer.openTSDBContext.get.close()
    sparkContext.stop()

  }

}
