package com.utils.hbase

import java.io.{InputStreamReader, BufferedReader, IOException}

import scala.collection.convert.decorateAsScala._

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._

import org.rogach.scallop.ScallopConf

import org.slf4j.{LoggerFactory, Logger}

import scala.collection.mutable.ListBuffer

object DeleteRowKeys extends HbaseUtilApp {

  def logger: Logger = LoggerFactory.getLogger(this.getClass)

  object appConf extends ScallopConf(args) {
    lazy val tableName = opt[String] (
      "tablename",
      required = true,
      descr    = "Hbase Table Name"
    )
    lazy val idFile = opt[String] (
      "idfile",
      required = true,
      descr    = "HDFS File containing the ids to be deleted"
    )

    afterInit
  }

  /** method to get the rowkey from given string - prefix with hash as stored in table */
  // entityId is our internal function (you can replace with your own row key id manipulation
  def getRowKey(id: String) = entityId(id)

  /** delete the rows for a given List of ids */
  def deleteRowsWithKeyList(
    conf: Configuration,
    hTableName: String,
    idList: List[String]) = {
    logger.info(s"Delete key list called with a list of ${idList.size} ids")
    val deleteList = new java.util.ArrayList[Delete]()
    idList.foreach(x => deleteList.add(new Delete(getRowKey(x))))
    usingHBaseTable(conf, hTableName) {
      hTable => {
        hTable.delete(deleteList)
        logger.info(s"Ids which are not deleted : ${deleteList.asScala.map(x => extractEntityIdAsString(x.getRow))}")
      }
    }
  }

  /*
   * reads the ids (strings) from the hdfs file and invoke the deleteRowWithKey method one by one
   */
  def deleteRowKeys() = {
    val p                  = new Path(appConf.idFile())
    val table              = appConf.tableName()

    if(! fs.exists(p)) {
      logger.error(s"${appConf.idFile()} does not exist")
      System.exit(-1)
    }

    var br: BufferedReader = null
    try {
      br            = new BufferedReader(new InputStreamReader(fs.open(p)))
      var idListBuf = ListBuffer.empty[String]
      var idStr     = br.readLine()
      var deleteCnt = 0

      while (idStr != null) {
        idListBuf += idStr.trim()
        if (idListBuf.size > 500) {
          logger.info(s"Deleting batch ${deleteCnt}")
          deleteCnt += 1
          val idList = idListBuf.toList
          deleteRowsWithKeyList(configuration, table, idList)
          idListBuf.clear()
        }
        idStr = br.readLine()
      }
      logger.info("Completed Reading the idfile")

      if (idListBuf.size > 0) {
        deleteRowsWithKeyList(configuration, table, idListBuf.toList)
        idListBuf.clear()
      }
    } catch {
      case ne: NullPointerException => {
        logger.error("Null pointer")
        ne.printStackTrace()
      }
      case ioe: IOException => {
        logger.error("Can't read the Id File/Operation failed. Please investigate")
        ioe.printStackTrace()
      }
      case e: Exception     => {
        e.printStackTrace()
      }
    }
    finally {
      if (br != null) br.close()
    }
  }

  /** run the deleteRowKeys */
  override def runApp () = deleteRowKeys
}

