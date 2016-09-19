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

package com.cgnal.openTSDB

import com.cgnal.kafkaAvro.consumers.SparkStreamingAvroConsumer
import com.cgnal.spark.opentsdb.{DataPoint, OpenTSDBContext, TSDBClientFactory, TSDBClientManager}
import com.typesafe.config.ConfigFactory
import net.opentsdb.core.TSDB
import net.opentsdb.utils.Config
import org.apache.commons.pool2.impl.SoftReferenceObjectPool
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}
import shaded.org.hbase.async.HBaseClient

/**
 * Created by cgnal on 15/09/16.
 */
class OpenTSDBWriter(openTSDBContext: OpenTSDBContext) extends Serializable{

  //val openTSDBContext: OpenTSDBContext = new OpenTSDBContext(sqlContext, Some(hadoopConf))

  def writeStream(stream : DStream[DataPoint[Double]]) = openTSDBContext.streamWrite(stream)


}

object WriterMain {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName("kafkaStreaming-consumer-test")
      .setMaster("local[*]")
      .set("spark.driver.allowMultipleContexts", "true")
    //.setSparkHome("dir.getAbsolutePath")

    val streamingContext: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    val topic = ConfigFactory.load().getString("spark-opentsdb-exmaples.kafka.topic")
    val brokers = ConfigFactory.load().getString("spark-opentsdb-exmaples.kafka.docker.brokers")
    val props = Map("metadata.broker.list" -> brokers)

    val hadoopConf = HBaseConfiguration.create()
    hadoopConf.set("hbase.master", "127.0.0.1:60000")
    hadoopConf.set("hbase.zookeeper.quorum", "127.0.0.1")
    hadoopConf.set("hbase.zookeeper.property.clientPort", "2181")

    val sparkContext: SparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)

    val openTSDBContext = new OpenTSDBContext(sqlContext, Some(hadoopConf))

    val openTSDBWriter = new OpenTSDBWriter(openTSDBContext)
    val consumer = new SparkStreamingAvroConsumer()

    val stream = consumer.run(streamingContext, Set(topic), props)
    val stream2: DStream[DataPoint[Double]] = stream.map(el => DataPoint(el.measure, el.timestamp, el.timestamp.toDouble, el.tags))
    openTSDBWriter.writeStream(stream2)

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}

