package com.cgnal.kafkaAvro.consumers.example

import com.cgnal.kafkaAvro.consumers.{OpenTSDBConsumer, SparkStreamingAvroConsumer}
import com.cgnal.kafkaAvro.converters.{EventConverter, SimpleEventConverter}
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.util.Try

/**
  * Created by cgnal on 20/09/16.
  */
object OpenTSDBConsumerMain{
  val logger = LoggerFactory.getLogger(this.getClass)
  var consumer: OpenTSDBConsumer[SimpleEventConverter] = _



  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().
      setAppName("spark-opentsdb-local-test")
    //.set("spark.io.compression.codec", "lzf")

    var config = ConfigFactory.load()


    val test = args match {
      case Array(testMode: String, keytab:String, principal:String) =>
        config = config
          .withValue("spark-opentsdb-exmaples.openTSDBContext.keytab",   ConfigValueFactory.fromAnyRef(keytab))
          .withValue("spark-opentsdb-exmaples.openTSDBContext.principal",   ConfigValueFactory.fromAnyRef(principal))


        logger.info(s"kafka: ${config.getString("spark-opentsdb-exmaples.kafka.brokers")}")
        logger.info(s"zookeeper: ${config.getString("spark-opentsdb-exmaples.zookeeper.host")}")
        logger.info(s"hbase: ${config.getString("spark-opentsdb-exmaples.hbase.master")}")

        println("Configurations: ")
        println(s"kafka: ${config.getString("spark-opentsdb-exmaples.kafka.brokers")}")
        println(s"zookeeper: ${config.getString("spark-opentsdb-exmaples.zookeeper.host")}")
        println(s"hbase: ${config.getString("spark-opentsdb-exmaples.hbase.master")}")

        testMode.toBoolean
      case _ =>
        true
    }

    if (test)
      conf.setMaster("local[*]")


    val hbaseMaster = config.getString("spark-opentsdb-exmaples.hbase.master")
    val Array(zkQuorum, zkPort) = config.getString("spark-opentsdb-exmaples.zookeeper.host").split(":")

    val hadoopConf = HBaseConfiguration.create()

    val sparkContext: SparkContext = new SparkContext(conf)
    val ssc = new StreamingContext(sparkContext, Milliseconds(200))
    val topic = config.getString("spark-opentsdb-exmaples.kafka.topic")
    val brokers = config.getString("spark-opentsdb-exmaples.kafka.brokers")
    val props = Map("metadata.broker.list" -> brokers)
    val keyTab = Try(config.getString("spark-opentsdb-exmaples.openTSDBContext.keytab"))
    val principal = Try(config.getString("spark-opentsdb-exmaples.openTSDBContext.principal"))

    if(keyTab.isSuccess && principal.isSuccess) {
      println("running the consumer with tabkey and principal")
      consumer = new OpenTSDBConsumer[SimpleEventConverter](ssc, hadoopConf, Set(topic), props, keyTab.get, principal.get)
    }
    else{
      consumer = new OpenTSDBConsumer[SimpleEventConverter](ssc, hadoopConf, Set(topic), props)
    }

    consumer.run()

    ssc.start()
    ssc.awaitTermination()
  }


}
