package com.cgnal.kafkaAvro.consumers.example

import com.cgnal.kafkaAvro.consumers.{OpenTSDBConsumer, SparkStreamingAvroConsumer}
import com.cgnal.spark.opentsdb.OpenTSDBContext
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
  * Created by cgnal on 20/09/16.
  */
object OpenTSDBConsumerMain extends App{
 val logger = LoggerFactory.getLogger(this.getClass)

 val conf = new SparkConf().
   setAppName("spark-opentsdb-local-test").
   setMaster("local[*]").
   set("spark.io.compression.codec", "lzf")

 val hbaseMaster = ConfigFactory.load().getString("spark-opentsdb-exmaples.hbase.master")
 val Array(zkQuorum, zkPort) =  ConfigFactory.load().getString("spark-opentsdb-exmaples.zookeeper.host").split(":")

 val hadoopConf = HBaseConfiguration.create()
 hadoopConf.set("hbase.master", hbaseMaster)
 hadoopConf.set("hbase.zookeeper.quorum", zkQuorum)
 hadoopConf.set("hbase.zookeeper.property.clientPort", zkPort)

 val sparkContext: SparkContext = new SparkContext(conf)
 val ssc = new StreamingContext(sparkContext, Milliseconds(200))
 val topic = ConfigFactory.load().getString("spark-opentsdb-exmaples.kafka.topic")
 val brokers = ConfigFactory.load().getString("spark-opentsdb-exmaples.kafka.docker.brokers")
 val props = Map("metadata.broker.list" -> brokers)

 val consumer = new OpenTSDBConsumer()
 consumer.run(ssc, hadoopConf, Set(topic), props)

 ssc.start()
 ssc.awaitTermination()


}
