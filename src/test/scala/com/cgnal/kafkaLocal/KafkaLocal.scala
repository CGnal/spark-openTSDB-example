package com.cgnal.kafkaLocal

import java.io.File
import java.net.InetSocketAddress
import java.util.Properties
import kafka.server.{KafkaConfig, KafkaServer}
import org.apache.zookeeper.server._
import scala.concurrent.Future
import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by cgnal on 12/09/16.
  */
object KafkaLocal{
  var zkServer: Option[ServerCnxnFactory] = None
  var kafkaServer: Option[KafkaServer] = None

  private def startZK(): Unit = {
    if(zkServer.isEmpty) {
      val dataDirectory = System.getProperty("java.io.tmpdir")
      val dir = new File(dataDirectory, "zookeeper").getAbsoluteFile
      try {
        val tickTime = 5000
        val server = new ZooKeeperServer(dir, dir, tickTime)
        val factory = ServerCnxnFactory.createFactory
        factory.configure(new InetSocketAddress("0.0.0.0", 2181), 1024)
        factory.startup(server)
        println("ZOOKEEPER server up!!")
        zkServer = Some(factory)


      } catch {
        case ex: Exception => System.err.println(s"Error in zookeeper server: ${ex.printStackTrace()}")
      }
      finally {dir.deleteOnExit()}
    } else println("ZOOKEEPER is already up")
  }

  private def stopZK() = {
    if (zkServer.isDefined) {zkServer.get.shutdown()}
    println("ZOOKEEPER server stopped")
  }

  private def startKafka() = {
    if(kafkaServer.isEmpty) {
      val dataDirectory = System.getProperty("java.io.tmpdir")
      val dir = new File(dataDirectory, "kafka").getAbsoluteFile
      try {
        val props = new Properties()
        //props.setProperty("hostname", "localhost")
        props.setProperty("port", "9092")
        props.setProperty("brokerid", "0")
        props.setProperty("log.dir", dir.getAbsolutePath)
        props.setProperty("enable.zookeeper", "true")
        props.setProperty("zookeeper.connect", "localhost:2181")

        // flush every message.
        props.setProperty("log.flush.interval", "1")

        // flush every 1ms
        props.setProperty("log.default.flush.scheduler.interval.ms", "1")

        val server: KafkaServer = new KafkaServer(new KafkaConfig(props))
        server.startup()
        println("KAFKA server on!!")
        kafkaServer = Some(server)
      } catch {
        case ex: Exception => System.err.println(s"Error in kafka server: ${ex.getMessage}")
      }
      finally {
        dir.deleteOnExit()}
    } else println("KAFKA is already up")
  }

  private def stopKafka(): Unit ={
    if(kafkaServer.isDefined) kafkaServer.get.shutdown()
    println("KAFKA server stopped")
  }

  def start(): Unit = {
    startZK()
    Thread.sleep(5000)
    startKafka()
  }

  def stop(): Unit = {
    stopKafka()
    Thread.sleep(2000)
    stopZK()
  }

}

object Main extends App {
  KafkaLocal.start()
  //Thread.sleep(10000)
 // KafkaLocal.stop()

}
