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

package com.cgnal.services

import java.io.File
import java.net.InetSocketAddress
import java.util
import java.util.Properties

import com.typesafe.config.ConfigFactory
import kafka.admin.AdminUtils
import kafka.server.{KafkaConfig, KafkaServer}
import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.zookeeper.server._
import org.slf4j.LoggerFactory

import collection.JavaConverters._

/**
 * Created by cgnal on 12/09/16.
 *
 * Run an local instance of zookeeper (localhost:2181) and kafka(localhost:9292)
 */
class KafkaLocal(var runZookeeper: Boolean = true) {

  var zkServer: Option[ServerCnxnFactory] = None
  var kafkaServer: Option[KafkaServer] = None
  val logger = LoggerFactory.getLogger(this.getClass)

  private def startZK(): Unit = {
    if (zkServer.isEmpty) {

      val dataDirectory = System.getProperty("java.io.tmpdir")
      val dir = new File(dataDirectory, "zookeeper")
      println(dir.toString)
      if (dir.exists())
        FileUtils.deleteDirectory(dir)

      try {
        val tickTime = 10000
        val server = new ZooKeeperServer(dir.getAbsoluteFile, dir.getAbsoluteFile, tickTime)
        val factory = ServerCnxnFactory.createFactory
        factory.configure(new InetSocketAddress("0.0.0.0", 2181), 1024)
        factory.startup(server)
        logger.info("ZOOKEEPER server up!!")
        zkServer = Some(factory)

      } catch {
        case ex: Exception => System.err.println(s"Error in zookeeper server: ${ex.printStackTrace()}")
      } finally { dir.deleteOnExit() }
    } else logger.info("ZOOKEEPER is already up")
  }

  private def stopZK() = {
    if (zkServer.isDefined) {
      zkServer.get.shutdown()
    }
    logger.info("ZOOKEEPER server stopped")
  }

  private def startKafka() = {
    if (kafkaServer.isEmpty) {
      val dataDirectory = System.getProperty("java.io.tmpdir")
      val dir = new File(dataDirectory, "kafka")
      if (dir.exists())
        FileUtils.deleteDirectory(dir)
      try {
        val props = new Properties()
        //props.setProperty("hostname", "localhost")
        props.setProperty("port", "9092")
        props.setProperty("broker.id", "0")
        props.setProperty("log.dir", dir.getAbsolutePath)
        props.setProperty("enable.zookeeper", "true")
        props.setProperty("zookeeper.connect", "localhost:2181")
        props.setProperty("advertised.host.name", "localhost")
        props.setProperty("connections.max.idle.ms", "9000000")
        props.setProperty("zookeeper.connection.timeout.ms", "10000")
        props.setProperty("zookeeper.session.timeout.ms", "10000")

        // flush every message.
        props.setProperty("log.flush.interval", "100")

        // flush every 1ms
        props.setProperty("log.default.flush.scheduler.interval.ms", "1000")

        val server = new KafkaServer(new KafkaConfig(props, false))
        server.startup()
        logger.info("KAFKA server on!!")

        kafkaServer = Some(server)
      } catch {
        case ex: Exception => System.err.println(s"Error in kafka server: ${ex.getMessage}")
      } finally {
        dir.deleteOnExit()
      }
    } else logger.info("KAFKA is already up")
  }

  private def stopKafka(): Unit = {
    if (kafkaServer.isDefined) {
      kafkaServer.get.shutdown()

      logger.info(s"KafkaServer ${kafkaServer.get.config.hostName} run state is: ${kafkaServer.get.kafkaController.isActive()} ")
    }
    logger.info("KAFKA server stopped")
  }

  def createTopic(topic: String): Unit = {
    AdminUtils.createTopic(kafkaServer.get.zkUtils, topic, 3, 1, new Properties())
    logger.info(s"Topic $topic created!")
  }

  def start(): Unit = {
    if(runZookeeper){
      startZK()
      Thread.sleep(5000)
    }

    startKafka()
  }

  def stop(): Unit = {
    stopKafka()
    if(runZookeeper){
      Thread.sleep(2000)
      stopZK()
    }

  }

}

object Main extends App {
  //echo stat | nc <zookeeper ip> 2181
  //echo mntr | nc <zookeeper ip> 2181
  // echo isro  | nc <zookeeper ip> 2181
  val kfServer = new KafkaLocal(true)
  kfServer.start()
  val topic = ConfigFactory.load().getString("spark-opentsdb-exmaples.kafka.topic")
  //kfServer.createTopic(topic)

  //Thread.sleep(10000)
  //kfServer.stop()

}
