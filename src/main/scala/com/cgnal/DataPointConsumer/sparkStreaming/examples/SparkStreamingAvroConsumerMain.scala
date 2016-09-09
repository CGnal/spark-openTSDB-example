package com.cgnal.DataPointConsumer.sparkStreaming.examples

import com.cgnal.DataPointConsumer.sparkStreaming.SparkStreamingAvroConsumer
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by cgnal on 09/09/16.
  */
object SparkStreamingAvroConsumerMain extends App {
  val sparkConf = new SparkConf().setAppName("kafkaStreaming-consumer").setMaster("local[*]")
  val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

  val topic = ConfigFactory.load().getString("spark-opentsdb-exmaples.kafka.topic")
  val brokers = ConfigFactory.load().getString("spark-opentsdb-exmaples.kafka.docker.brokers")
  val props = Map("metadata.broker.list" -> brokers)

  new SparkStreamingAvroConsumer().run(ssc, Set(topic), props)

  ssc.start()
  ssc.awaitTermination()


}