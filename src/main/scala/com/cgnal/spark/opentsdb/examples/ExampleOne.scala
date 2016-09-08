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

package com.cgnal.spark.opentsdb.examples

import java.io.File
import java.sql.Timestamp
import java.time.Instant

import com.cgnal.spark.opentsdb.{ DataPoint, OpenTSDBContext, _ }
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.dstream.{ DStream, InputDStream }
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.{ SparkConf, SparkContext }

import scala.util.Random

/*
spark-submit --executor-memory 1200M \
  --driver-class-path /etc/hbase/conf \
  --conf spark.executor.extraJavaOptions=-Djava.security.auth.login.config=/tmp/jaas.conf \
  --conf spark.executor.extraClassPath=/etc/hbase/conf \
  --master yarn --deploy-mode client \
  --class com.cgnal.spark.opentsdb.examples.ExampleOne spark-opentsdb-examples-assembly-1.0.jar xxxx dgreco.keytab dgreco@DGRECO-MBP.LOCAL
 */

object ExampleOne extends App {

  val yarn = false

  val initialExecutors = 4

  val minExecutors = 4

  val conf = new SparkConf().setAppName("spark-cdh5-template-yarn")

  val master = conf.getOption("spark.master")

  val uberJarLocation = {
    val location = getJar(ExampleOne.getClass)
    if (new File(location).isDirectory) s"${System.getProperty("user.dir")}/assembly/target/scala-2.10/spark-opentsdb-examples-assembly-1.0.jar" else location
  }

  if (master.isEmpty) {
    //it means that we are NOT using spark-submit

    addPath(args(0))

    if (yarn)
      conf.
        setMaster("yarn-client").
        setAppName("spark-cdh5-template-yarn").
        setJars(List(uberJarLocation)).
        set("spark.yarn.jar", "local:/opt/cloudera/parcels/CDH/lib/spark/assembly/lib/spark-assembly.jar").
        set("spark.executor.extraClassPath", "/etc/hbase/conf"). //This is an hack I made I'm not sure why it works though
        set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
        set("spark.io.compression.codec", "lzf").
        set("spark.speculation", "true").
        set("spark.shuffle.manager", "sort").
        set("spark.shuffle.service.enabled", "true").
        set("spark.dynamicAllocation.enabled", "true").
        set("spark.dynamicAllocation.initialExecutors", Integer.toString(initialExecutors)).
        set("spark.dynamicAllocation.minExecutors", Integer.toString(minExecutors)).
        set("spark.executor.cores", Integer.toString(1)).
        set("spark.executor.memory", "1200m")
    else
      conf.
        setAppName("spark-cdh5-template-local").
        setMaster("local[4]")
  }

  val sparkContext = new SparkContext(conf)

  implicit val sqlContext: SQLContext = new SQLContext(sparkContext)

  sparkContext.setLogLevel("ERROR")

  OpenTSDBContext.saltWidth = 1
  OpenTSDBContext.saltBuckets = 4

  val openTSDBContext = new OpenTSDBContext(sqlContext)

  openTSDBContext.keytabLocalTempDir = "/tmp"
  openTSDBContext.keytab = args(1)
  openTSDBContext.principal = args(2)

  val ssc = new StreamingContext(sparkContext, Seconds(5))
  val brokers = "happy:9092,sleepy:9092:doc:9092,grumpy:9092"
  val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
  val messages: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set("test"))

  val lines: DStream[String] = messages.map(_._2)

  val dataPoints: DStream[DataPoint[Double]] = lines.transform[DataPoint[Double]]((rdd: RDD[String]) => rdd.mapPartitions[DataPoint[Double]] {
    iterator =>
      val r = new Random
      val M = 10
      val V = 100
      iterator map {
        line =>
          val ts = Timestamp.from(Instant.parse(s"2000-07-05T10:00:00.00Z"))
          val epoch = ts.getTime + line.toLong
          val value = r.nextInt(V).toDouble
          val metric = s"mymetric${r.nextInt(M)}"
          DataPoint(metric, epoch, value, Map("key1" -> "value1", "key2" -> "value2"))
      }
  })

  openTSDBContext.streamWrite(dataPoints)

  ssc.start()
  ssc.awaitTermination()

  sparkContext.stop()

}
