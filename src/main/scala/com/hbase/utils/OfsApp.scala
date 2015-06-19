package com.utils.hbase 

import java.io.{Reader, InputStreamReader, BufferedReader}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.HBaseConfiguration

import org.apache.hadoop.hbase.client.{HTableInterface, HConnectionManager}

trait OfsApp {
  private var argsFromMain: Option[Array[String]] = None

  val configuration = new Configuration
  lazy val fs       = FileSystem.get(configuration)

  def args = argsFromMain.getOrElse(sys.error("Arguments not given"))

  def runApp ()

  def main(args: Array[String]): Unit = {
    argsFromMain = Some(args)
    runApp()
  }

  /*
   * function to run hbase client operations
   */
  def usingHBaseTable[T](conf: Configuration = configuration,tableName: String)(f: HTableInterface => T) = {
    val hbaseConf  = HBaseConfiguration.create(conf)
    val connection = HConnectionManager.createConnection(hbaseConf)
    val table      = connection.getTable(tableName)
    try {
      f(table)
    }
    finally {
      table.close()
      connection.close()
    }
  }
}
