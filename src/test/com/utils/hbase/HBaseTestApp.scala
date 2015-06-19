package com.utils.hbase

import org.apache.hadoop.hbase.{HBaseConfiguration, HBaseTestingUtility}

import org.specs2.Specification

/*
 * HBase Test App. A helper to start hbase mini cluster for test automation
 */
abstract class HBaseTestApp extends Specification {

  private var hbaseTestUtil: HBaseTestingUtility = _

  val conf = HBaseConfiguration.create()

  /*
   * Function to start hbase mini cluster
   *
   * return hbaseTestUtil and can be used to obtain other features from HBaseTestingUtility
   */
  def initCluster(): HBaseTestingUtility = {
    hbaseTestUtil = new HBaseTestingUtility(conf)
    hbaseTestUtil.startMiniCluster(1, 1)
    hbaseTestUtil
  }

  /*
   * Function to create an hbase table. This is minimal usage.
   */
  def createTable(tableName: String, colFamily: String) = {
    hbaseTestUtil.createTable(tableName, colFamily)
  }

  /*
   * Function to shut down the mini cluster
   */
  def shutCluster() = hbaseTestUtil.shutdownMiniCluster()
}
