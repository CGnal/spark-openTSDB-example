package com.cgnal.openTSDB
import java.sql.Timestamp
import java.time.Instant

import com.cgnal.spark.opentsdb.{DataPoint, OpenTSDBContext}
import com.typesafe.config.ConfigFactory
import net.opentsdb.core.TSDB
import net.opentsdb.utils.Config
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import shaded.org.hbase.async.HBaseClient

import scala.collection.mutable

/**
  * Created by cgnal on 15/09/16.
  */
object HBaseLocal {

  def getConf(): Config = {
    OpenTSDBContext.saltWidth = 1
    OpenTSDBContext.saltBuckets = 4


    val saltWidth = ConfigFactory.load().getString("spark-opentsdb-exmaples.openTSDB.saltWidth").toInt
    val saltBuckets = ConfigFactory.load().getString("spark-opentsdb-exmaples.openTSDB.saltBuckets")
    val table = ConfigFactory.load().getString("spark-opentsdb-exmaples.openTSDB.hbaseTable")
    val uidTable = ConfigFactory.load().getString("spark-opentsdb-exmaples.openTSDB.hbaseUidTable")
    val autoCreate = ConfigFactory.load().getString("spark-opentsdb-exmaples.openTSDB.autoCreateMetrics")



    val config = new Config(false)
    config.overrideConfig("tsd.storage.hbase.data_table", table)
    config.overrideConfig("tsd.storage.hbase.uid_table", uidTable)
    config.overrideConfig("tsd.core.auto_create_metrics", autoCreate)

    if (saltWidth > 0) {
      config.overrideConfig("tsd.storage.salt.width", saltWidth.toString)
      config.overrideConfig("tsd.storage.salt.buckets", saltBuckets)
    }
    config.disableCompactions()
    config
  }

  def main(args: Array[String]): Unit = {


    //val hbaseAsyncClient = new HBaseClient("localhost:2181", "/hbase")
    val conf = new SparkConf().
      setAppName("spark-opentsdb-local-test").
      setMaster("local[*]").
      set("spark.io.compression.codec", "lzf")

    val hadoopConf = HBaseConfiguration.create()
    hadoopConf.set("hbase.master", "127.0.0.1:60000")
    hadoopConf.set("hbase.zookeeper.quorum", "127.0.0.1")
    hadoopConf.set("hbase.zookeeper.property.clientPort", "2181")

    val sparkContext: SparkContext = new SparkContext(conf)
    val sqlContext = new SQLContext(sparkContext)

    //it shoul be useless
    val hbaseAsyncClient = new HBaseClient("127.0.0.1:2181", "/hbase")
    val tsdb = new TSDB(hbaseAsyncClient, getConf())

    val openTSDBContext = new OpenTSDBContext(sqlContext, Some(hadoopConf))
    //val tsdb = new TSDB()

    val streamingContext = new StreamingContext(sparkContext, Milliseconds(200))

    val points = for {
      i <- 0 until 10
      ts = Timestamp.from(Instant.parse(s"2016-07-05T${10 + i}:00:00.00Z"))
      epoch = ts.getTime
      point = DataPoint("mymetric3", epoch, i.toDouble, Map("key1" -> "value1", "key2" -> "value2"))
    } yield point

    val rdd = sparkContext.parallelize[DataPoint[Double]](points)

    @SuppressWarnings(Array("org.wartremover.warts.MutableDataStructures"))
    val stream = streamingContext.queueStream[DataPoint[Double]](mutable.Queue(rdd))

    openTSDBContext.streamWrite(stream)
    print(rdd.take(100))

    streamingContext.start()

    Thread.sleep(1000)

    val tsStart = Timestamp.from(Instant.parse(s"2016-07-05T10:00:00.00Z"))
    val tsEnd = Timestamp.from(Instant.parse(s"2016-07-05T20:00:00.00Z"))

    //val df = openTSDBContext.loadDataFrame("mymetric3", Map("key1" -> "value1", "key2" -> "value2"), None)


    //Thread.sleep(2000)
    streamingContext.stop(false, false)

    //  println(df.schema)

    sparkContext.stop()
  }
}
