package com.cgnal.kafkaAvro.consumers.example

import com.cgnal.kafkaAvro.consumers.{OpenTSDBConsumer, SparkStreamingAvroConsumer}
import com.cgnal.spark.opentsdb.OpenTSDBContext
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
  * Created by cgnal on 20/09/16.
  */
object OpenTSDBConsumerMain{
 val logger = LoggerFactory.getLogger(this.getClass)

 def main(args: Array[String]): Unit = {

  val conf = new SparkConf().
    setAppName("spark-opentsdb-local-test").
    set("spark.io.compression.codec", "lzf")

  var config = ConfigFactory.load()

  val test = args match {
   case Array(testMode: String, zkHostIp: String, kafkaBrokers: String, hbaseHostIP:String) =>
    config = config
      .withValue("spark-opentsdb-exmaples.zookeeper.host",  ConfigValueFactory.fromAnyRef(zkHostIp))
      .withValue("spark-opentsdb-exmaples.kafka.brokers",   ConfigValueFactory.fromAnyRef(kafkaBrokers))
      .withValue("spark-opentsdb-exmaples.hbase.master",   ConfigValueFactory.fromAnyRef(hbaseHostIP))

    logger.info("Changed default config in")
    logger.info(s"\t kafka: ${config.getString("spark-opentsdb-exmaples.kafka.brokers")}")
    logger.info(s"\t zookeeper: ${config.getString("spark-opentsdb-exmaples.zookeeper.host")}")
    logger.info(s"\t hbase: ${config.getString("spark-opentsdb-exmaples.hbase.master")}")
    testMode.toBoolean
   case _ => true
  }

  if (test)
   conf.setMaster("local[*]")
  else
   conf.setMaster("master")


  val hbaseMaster = config.getString("spark-opentsdb-exmaples.hbase.master")
  val Array(zkQuorum, zkPort) = config.getString("spark-opentsdb-exmaples.zookeeper.host").split(":")

  val hadoopConf = HBaseConfiguration.create()
  hadoopConf.set("hbase.master", hbaseMaster)
  hadoopConf.set("hbase.zookeeper.quorum", zkQuorum)
  hadoopConf.set("hbase.zookeeper.property.clientPort", zkPort)

  val sparkContext: SparkContext = new SparkContext(conf)
  val ssc = new StreamingContext(sparkContext, Milliseconds(200))
  val topic = config.getString("spark-opentsdb-exmaples.kafka.topic")
  val brokers = config.getString("spark-opentsdb-exmaples.kafka.brokers")
  val props = Map("metadata.broker.list" -> brokers)

  val consumer = new OpenTSDBConsumer()
  consumer.run(ssc, hadoopConf, Set(topic), props)

  ssc.start()
  ssc.awaitTermination()
 }


}
