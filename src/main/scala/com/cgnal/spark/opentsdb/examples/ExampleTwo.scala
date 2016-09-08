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

import com.cgnal.spark.opentsdb.{ OpenTSDBContext, _ }
import org.apache.spark.sql.SQLContext
import org.apache.spark.{ SparkConf, SparkContext }

/*
spark-submit --executor-memory 1200M \
  --driver-class-path /etc/hbase/conf \
  --conf spark.executor.extraJavaOptions=-Djava.security.auth.login.config=/tmp/jaas.conf \
  --conf spark.executor.extraClassPath=/etc/hbase/conf \
  --master yarn --deploy-mode client \
  --class com.cgnal.spark.opentsdb.examples.ExampleTwo spark-opentsdb-examples-assembly-1.0.jar xxxx dgreco.keytab dgreco@DGRECO-MBP.LOCAL
 */

object ExampleTwo extends App {

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

  val M = 10

  val tot = (0 until M).map(i => {
    val df = sqlContext.read.options(Map(
      "opentsdb.metric" -> s"mymetric$i",
      "opentsdb.tags" -> "key1->value1,key2->value2",
      "opentsdb.keytabLocalTempDir" -> "/tmp",
      "opentsdb.keytab" -> args(1),
      "opentsdb.principal" -> args(2)
    )).opentsdb

    val start = System.currentTimeMillis()
    val count = df.count()
    val stop = System.currentTimeMillis()
    println(s"$count data point retrieved in ${stop - start} milliseconds")
    count
  }).sum

  println(tot)

  sparkContext.stop()

}
