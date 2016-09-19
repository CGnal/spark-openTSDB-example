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
import com.typesafe.config.ConfigFactory
import kafka.admin.AdminUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{ Seconds, StreamingContext }

/**
 * Created by cgnal on 09/09/16.
 */
object SparkStreamingAvroConsumerMain extends App {

  val dataDirectory = System.getProperty("java.io.tmpdir")
  val dir = new File(dataDirectory, "hadoop")
  dir.deleteOnExit()

  System.setProperty("hadoop.home.dir", "/")
  val sparkConf = new SparkConf()
    .setAppName("kafkaStreaming-consumer-test")
    .setMaster("local[*]")
  //.setSparkHome("dir.getAbsolutePath")
  implicit val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

  val topic = ConfigFactory.load().getString("spark-opentsdb-exmaples.kafka.topic")
  val brokers = ConfigFactory.load().getString("spark-opentsdb-exmaples.kafka.docker.brokers")
  val props = Map("metadata.broker.list" -> brokers)

  val pippo = new SparkStreamingAvroConsumer().run(ssc, Set(topic), props)

  pippo.print(100)


}