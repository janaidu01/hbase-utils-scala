package com.utils.hbase

import java.util

import org.apache.hadoop.mapreduce.Job

import org.apache.hadoop.mapreduce.Mapper

import org.apache.hadoop.conf.Configuration

import org.apache.hadoop.fs.{ FileSystem, Path }

import org.apache.hadoop.mapreduce.lib.input.{TextInputFormat, FileInputFormat}

import org.apache.hadoop.mapreduce.lib.output.{TextOutputFormat, FileOutputFormat}

import org.apache.hadoop.io.{ NullWritable, Text }

import org.apache.hadoop.hbase.HBaseConfiguration

import org.apache.hadoop.hbase.client._

import org.slf4j.{LoggerFactory, Logger}

class DeleteMapper extends Mapper[Object, Text, Text, NullWritable] {

  val logger: Logger          = LoggerFactory.getLogger(this.getClass)
  var connection: HConnection = _
  var table: HTableInterface  = _
  var idDeleteList            = new util.ArrayList[Delete]()

  /** Set up method - opens connection for Hbase table */
  override def setup(context:  Mapper[Object, Text, Text, NullWritable]#Context): Unit = {
    connection = HConnectionManager.createConnection(context.getConfiguration)
    table  = connection.getTable(context.getConfiguration.get("tablename"))
  }

  /** mapper - makes the list of ids (Bytes) to be deleted */
  override def map(
    key: Object,
    value: Text,
    context: Mapper[Object, Text, Text, NullWritable]#Context
  ): Unit = {

    // replace the id manipulation with your own function
    val deleteObj = new Delete(DeleteRowKeys.getRowKey(value.toString))
    idDeleteList.add(deleteObj)
  }

  /** Clean up method - calls delete for a list of ids and close the connection */
  override def cleanup(context:  Mapper[Object, Text, Text, NullWritable]#Context): Unit = {
    logger.info(s"Calling Delete, List size is ${idDeleteList.size()}")
    table.delete(idDeleteList)
    logger.info(s"Ids which are not deleted ${idDeleteList.toString}")
    logger.info(s"Number of IDs which are not deleted ${idDeleteList.size()}")

    table.close()
    connection.close()
  }
}

// Driver
// TBD arguments parsing using scallop
object DeleteRowKeysMapRedJob {
  def main (args: Array[String]): Unit = {
    //val logger: Logger = LoggerFactory.getLogger(this.getClass)

    val conf      = new Configuration()
    val hbaseConf = HBaseConfiguration.create(conf)
    val fs        = FileSystem.get(conf)
    val path      = new Path(args(0))
    hbaseConf.set("tablename", args(2))

    val job  = Job.getInstance(hbaseConf, "DeleteRowKeysMapRed")

    // job settings - It is a map only job
    job.setJarByClass(DeleteRowKeysMapRedJob.getClass)
    job.setMapperClass(classOf[DeleteMapper])
    job.setInputFormatClass(classOf[TextInputFormat])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[NullWritable])
    job.setOutputFormatClass(classOf[TextOutputFormat[Text, NullWritable]])
    job.setNumReduceTasks(0)

    FileInputFormat.addInputPath(job, path)
    // splitting the id file to 5 MBs to spawn more number of mappers
    FileInputFormat.setMaxInputSplitSize(job, 5 * 1024 * 1024)
    FileOutputFormat.setOutputPath(job, new Path(args(1)))
    job.waitForCompletion(true)
  }
}
