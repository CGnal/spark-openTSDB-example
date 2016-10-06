/*
 * Copyright 2016 CGnal S.p.A.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cgnal.kafkaAvro.consumers.example

import java.io.File
import java.util.Properties

import com.cgnal.kafkaAvro.consumers.SparkStreamingAvroConsumer
import com.cgnal.kafkaAvro.converters.SimpleEventConverter
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import kafka.admin.AdminUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory


/**
  * Created by cgnal on 09/09/16.
  */
object SparkStreamingAvroConsumerMain  {
  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().
      setAppName("spark-opentsdb-local-test")
    //set("spark.io.compression.codec", "lzf")

    var config = ConfigFactory.load()

    val test = args match {
      case Array(testMode: String) =>
        logger.info(s"kafka: ${config.getString("spark-opentsdb-exmaples.kafka.brokers")}")
        logger.info(s"zookeeper: ${config.getString("spark-opentsdb-exmaples.zookeeper.host")}")
        testMode.toBoolean

      case _ => true
    }

    if (test)
      sparkConf.setMaster("local[*]")

    implicit val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    val topic = ConfigFactory.load().getString("spark-opentsdb-exmaples.kafka.topic")
    val brokers = ConfigFactory.load().getString("spark-opentsdb-exmaples.kafka.brokers")
    val props = Map("metadata.broker.list" -> brokers)

    val stream = new SparkStreamingAvroConsumer[SimpleEventConverter](ssc, Set(topic), props).run()

    stream.print(100)

    ssc.start()
    ssc.awaitTermination()
  }

}