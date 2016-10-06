package com.cgnal.kafkaAvro
import java.io.File
import java.util.Properties

import com.cgnal.kafkaAvro.consumers.SparkStreamingAvroConsumer
import com.cgnal.kafkaAvro.converters.SimpleEventConverter
import com.cgnal.kafkaAvro.producers.KafkaAvroProducer
import com.cgnal.services.{HbaseLocal, KafkaLocal}
import com.cgnal.spark.opentsdb.DataPoint
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._

import scala.collection.JavaConversions._
/**
  * Created by cgnal on 04/10/16.
  */
class KafkaAvroSpec extends WordSpec with MustMatchers with BeforeAndAfterAll {
  val topic = "test-spec"
  val metric = ConfigFactory.load().getString("spark-opentsdb-exmaples.openTSDB.metric").toInt
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

  var sparkContext: SparkContext = _
  implicit var streamingContext: StreamingContext = _
  val producer = new KafkaAvroProducer()
  var consumer: SparkStreamingAvroConsumer[SimpleEventConverter] = _
  var hbase: HbaseLocal = _
  var kfServer: KafkaLocal = _




  override def beforeAll(): Unit = {

    val dataDirectory = System.getProperty("java.io.tmpdir")
    val dir = new File(dataDirectory, "hadoop")
    dir.deleteOnExit()
    System.setProperty("hadoop.home.dir", dir.getAbsolutePath)
    hbase = new HbaseLocal()
    hbase.start()
    Thread.sleep(10000)
    kfServer = new KafkaLocal(false)
    kfServer.start()
    kfServer.createTopic(topic)
    val conf = new SparkConf().
      setAppName("spark-opentsdb-example-test").
      setMaster("local[*]")

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    sparkContext = new SparkContext(conf)
    streamingContext = new StreamingContext(sparkContext, Milliseconds(500))
    consumer = new SparkStreamingAvroConsumer(streamingContext, Set(topic), propsConsumer)

  }

  "KafkaAvroSpec" must {
    "produce 200 data-points on topic test-spec" in {

      producer.run(2, 100, 0L, propsProducer, topic, metric)

      val propsTestConsumer = new Properties()
      propsTestConsumer.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
      propsTestConsumer.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
      propsTestConsumer.put("bootstrap.servers", "localhost:9092")
      propsTestConsumer.put("group.id", "consumer-test")
      propsTestConsumer.put("zookeeper.connect", "localhost:2181")
      propsTestConsumer.put("zookeeper.session.timeout.ms", "400")
      propsTestConsumer.put("zookeeper.sync.time.ms", "200")
      propsTestConsumer.put("auto.commit.interval.ms", "1000")

      val testConsumer: KafkaConsumer[Array[Byte], Array[Byte]] = new KafkaConsumer[Array[Byte], Array[Byte]](propsTestConsumer)
      val topics = testConsumer.listTopics()
      val data = testConsumer.poll(100)
      data.iterator().toList.size must be equals 200
      topics.containsKey(topic) must be equals true
      testConsumer.close()
    }

    "consume 200 data-point on topic test-spec" in {

      var resultsRDD = scala.collection.mutable.ArrayBuffer.empty[Array[DataPoint[Double]]]
      val thread = new Thread(new Runnable {
        override def run(): Unit = producer.run(2, 100, 500, propsProducer, topic, metric.toInt)
      })
      thread.start()

      val stream: DStream[DataPoint[Double]] = consumer.run()
      stream.foreachRDD { rdd =>
        resultsRDD += rdd.collect()
        ()
      }
      streamingContext.start()
      Thread.sleep(2000)
      streamingContext.stop(false, false)

      val resultArrayFromRDD = resultsRDD.flatten.toList

      resultArrayFromRDD.size must be equals 200

    }
  }

  override def afterAll(): Unit = {
    kfServer.stop()
    Thread.sleep(5000)
    hbase.stop()
    sparkContext.stop()
  }
}

