package com.cgnal.services

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.hbase.client.{ClusterConnection, ConnectionFactory, HBaseAdmin}
import org.apache.hadoop.hbase.{HBaseTestingUtility, _}
import org.apache.hadoop.hbase.spark.HBaseContext

/**
  * Created by cgnal on 19/09/16.
  */
class HbaseLocal {
  //var hBaseServer: Option[HBaseTestingUtility] = None
  val LOG = LogFactory.getLog(this.getClass)
  var  miniCluster: Option[HBaseTestingUtility] = None
  var hBaseAdmin: Option[HBaseAdmin] = None

  val tsdbUidTable = "tsdb-uid"
  val tsdbTable = "tsdb"
  val tsdbTreeTable = "tsdb-tree"
  val tsdbMetaTable = "tsdb-meta"

  def start() = {
    if (miniCluster.isEmpty) {

      val dataDirectory = System.getProperty("java.io.tmpdir")
      val dir = new File(dataDirectory, "hbase")
      println(dir.toString)
      if (dir.exists())
        FileUtils.deleteDirectory(dir)

      try
        miniCluster = Some(new HBaseTestingUtility())
        miniCluster.get.getConfiguration().set("test.hbase.zookeeper.property.clientPort", "2181");
        miniCluster.get.startMiniCluster()
        def configuration = miniCluster.get.getConfiguration

        //hBaseAdmin = Some(new HBaseAdmin(configuration))
        val conn = ConnectionFactory.createConnection(configuration)
        hBaseAdmin = Some(new HBaseAdmin(conn))

        try {

          val tsdbUid: HTableDescriptor = new HTableDescriptor(TableName.valueOf("tsdb-uid"))
          tsdbUid.addFamily(new HColumnDescriptor("id"))
          tsdbUid.addFamily(new HColumnDescriptor("name"))

          val tsdb = new HTableDescriptor(TableName.valueOf("tsdb"))
          tsdb.addFamily(new HColumnDescriptor("t"))

          val tsdbTree = new HTableDescriptor(TableName.valueOf("tsdb-tree"))
          tsdbTree.addFamily(new HColumnDescriptor("t"))

          val tsdbMeta = new HTableDescriptor(TableName.valueOf("tsdb-meta"))
          tsdbMeta.addFamily(new HColumnDescriptor("name"))

          conn.getAdmin.createTable(tsdbUid)
          conn.getAdmin.createTable(tsdb)
          conn.getAdmin.createTable(tsdbTree)
          conn.getAdmin.createTable(tsdbMeta)

          println(s"Table $tsdbUidTable is created: ${conn.getAdmin.tableExists(TableName.valueOf(tsdbUidTable))}")
          println(s"Table $tsdbTable is created: ${conn.getAdmin.tableExists(TableName.valueOf(tsdbTable))}")
          println(s"Table $tsdbTreeTable is created: ${conn.getAdmin.tableExists(TableName.valueOf(tsdbTreeTable))}")
          println(s"Table $tsdbMetaTable is created: ${conn.getAdmin.tableExists(TableName.valueOf(tsdbMetaTable))}")
        }
        //finally admin.close()
    } else println("HBASE is already up")
  }

  def stop() = {
    miniCluster.get.shutdownMiniCluster()
  }

}

object HBaseLocalMain extends App {

  val hbaseLocal = new HbaseLocal()
  hbaseLocal.start()

  //hbaseLocal.stop()

}