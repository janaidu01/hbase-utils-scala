package com.utils.hbase

import org.apache.hadoop.hbase.client.{HTable, Get, Put}
import org.apache.hadoop.hbase.util.Bytes

import org.specs2.specification.Step

class DeleteRowKeysSpec extends HBaseTestApp { def is = s2"""

  HBase Testing
  =============
  HBase testing should

  start the cluster ${Step(initCluster())}

  DeleteRowKeyWithList should always delete the list of ids given $deleteKeyWithList

  stop the cluster ${Step(shutCluster())}
  """
  /*
   * test for delete row with key
   * entityId is our internal function, replace with your own id manipulation
   */
  object inputData {
    val rowIdsStr = List("ABCD+000001", "ABCD+000002", "ABCD000001", "ABCD000002")

    // defaulting column family, qualifier and value for all
    val cf     = "cftest"
    val ql     = "ql1"
    val val1   = "val1"

    def loadData(table: String): HTable = {
      val hTable = createTable(table, inputData.cf)
      for (id <- inputData.rowIdsStr) {
        val hashId = entityId(id)
        val putObj = new Put(hashId)
        putObj.add(
          Bytes.toBytes(inputData.cf),
          Bytes.toBytes(inputData.ql),
          Bytes.toBytes(inputData.val1)
        )
        hTable.put(putObj)
      }
      hTable
    }
  }

  def deleteKeyWithList = {
    val hTable = inputData.loadData("table1")

    DeleteRowKeys.deleteRowsWithKeyList(conf, "table1", List("ABCD000001", "ABCD000002", "ABCD000003"))

    val exists: List[Boolean] = {
      for (id <- inputData.rowIdsStr)
      yield hTable.exists(new Get(entityId(id)))
    }

    hTable.flushCommits()

    exists mustEqual List(true, true, false, false)
  }
}
