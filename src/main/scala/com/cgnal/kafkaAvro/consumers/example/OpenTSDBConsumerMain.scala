package com.cgnal.kafkaAvro.consumers.example

import com.cgnal.kafkaAvro.consumers.{OpenTSDBConsumer, SparkStreamingAvroConsumer}
import com.cgnal.spark.opentsdb.OpenTSDBContext
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by cgnal on 20/09/16.
  */
object OpenTSDBConsumerMain extends App{

  val conf = new SparkConf().
    setAppName("spark-opentsdb-local-test").
    setMaster("local[*]").
    set("spark.io.compression.codec", "lzf")

  val hadoopConf = HBaseConfiguration.create()
  hadoopConf.set("hbase.master", "127.0.0.1:60000")
  hadoopConf.set("hbase.zookeeper.quorum", "127.0.0.1")
  hadoopConf.set("hbase.zookeeper.property.clientPort", "2181")

  val sparkContext: SparkContext = new SparkContext(conf)
  val ssc = new StreamingContext(sparkContext, Milliseconds(200))
  val topic = ConfigFactory.load().getString("spark-opentsdb-exmaples.kafka.topic")
  val brokers = ConfigFactory.load().getString("spark-opentsdb-exmaples.kafka.docker.brokers")
  val props = Map("metadata.broker.list" -> brokers)

  val consumer = new OpenTSDBConsumer()
    consumer.run(ssc, hadoopConf, Set(topic), props)
  //Thread.sleep(2000)

  //val df = consumer.openTSDBContext.get.loadDataFrame("metric", Map("tag"-> "hello"), None)

  //df.collect().foreach(el => println(el))

  ssc.start()
  ssc.awaitTermination()


}
