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

package com.cgnal.kafkaAvro.producers

import java.sql.Timestamp
import java.util.Properties

import com.cgnal.DataPoint
import com.gensler.scalavro.types.AvroType
import com.typesafe.config.ConfigFactory
import kafka.admin.AdminUtils
import org.I0Itec.zkclient.{ZkClient, ZkConnection}
import kafka.utils. ZkUtils
import kafka.utils.ZKStringSerializer$
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.slf4j.LoggerFactory

/**
  * Created by cgnal on 08/09/16.
  */
class KafkaAvroProducer {
  val logger = LoggerFactory.getLogger(this.getClass)

  /**
    *
    * @param messages number of messages to send to each interval
    * @param intervalTime time in milliseconds between two iterations of sending messages
    */
  def run(messages: Int, intervalTime: Long, props: Properties, topic: String): Unit = {

    val producer = new KafkaProducer[Array[Byte], Array[Byte]](props)
    val buf = new ByteArrayOutputStream()
    val schema = AvroType[DataPoint]
    val startTime = System.currentTimeMillis
    try {
      while (true) {

        for (i <- 0 to messages) {
          val ts = new Timestamp(System.currentTimeMillis() + (i * 1000L))
          val epoch = ts.getTime
          val data = DataPoint("metric", epoch, System.currentTimeMillis(), Map("tag" -> i.toString))

          schema.io.write(data, buf)
          val key = s"${data.timestamp}-${data.tags.getOrElse("tag", "-1")}"
          val message = new ProducerRecord[Array[Byte], Array[Byte]](topic, key.getBytes, buf.toByteArray)
          producer.send(message)
          buf.reset()
        }
        //logger.info(s"Written $messages messages")
        println(s"Written $messages messages, now await $intervalTime millisec")
        producer.flush()
        Thread.sleep(intervalTime)

      }
    } finally {
      producer.close()
      val time = (System.currentTimeMillis() - startTime) / 1000
      println(s"Kafka Producer closed in $time sec")

    }
  }

  def createTopic(topic:String, zkHost: String):Unit = {

    val zkClient = new ZkClient(zkHost, 10000, 10000, ZKStringSerializer$.MODULE$)
    val zkUtils = new ZkUtils(zkClient, new ZkConnection(zkHost), false)
    AdminUtils.createTopic(zkUtils, topic, 2, 1, new Properties())
  }

  /**
    *
    * @param messages number of messages to send to each interval
    * @param intervalTime time in milliseconds between two iterations of sending messages
    */
  def run(loops: Int, messages: Int, intervalTime: Long, props: Properties, topic: String): Unit = {

    val producer = new KafkaProducer[Array[Byte], Array[Byte]](props)
    val buf = new ByteArrayOutputStream()
    val schema = AvroType[DataPoint]
    val startTime = System.currentTimeMillis

    val metric = ConfigFactory.load().getString("spark-opentsdb-exmaples.openTSDB.metric")
    try {
      for (loop <- 0 until loops) {

        for (i <- 0 to messages) {
          val ts = new Timestamp(System.currentTimeMillis() + (i * 1000L))
          val epoch = ts.getTime
          val data = DataPoint(metric, epoch, i.toDouble, Map("tag" -> "hello"))

          schema.io.write(data, buf)
          val key = s"${data.timestamp}-${data.tags.getOrElse("tag", "-1")}"
          val message = new ProducerRecord[Array[Byte], Array[Byte]](topic, key.getBytes, buf.toByteArray)
          producer.send(message)
          buf.reset()
        }
        //logger.info(s"Written $messages messages")
        println(s"Written $messages messages, now await $intervalTime millisec")
        producer.flush()
        Thread.sleep(intervalTime)

      }
    } finally {
      producer.close()
      val time = (System.currentTimeMillis() - startTime) / 1000
      println(s"Kafka Producer closed in $time sec")
    }
  }

}
