package com.utils.hbase

import org.apache.hadoop.conf.Configuration

import org.apache.hadoop.fs.{FsShell, Path, FileSystem}

import org.apache.hadoop.io.{ BytesWritable, NullWritable, Text }

import org.apache.hadoop.mapreduce.Job

import org.apache.hadoop.mapreduce.Mapper

import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import org.apache.hadoop.hbase.client.HTable

import org.apache.hadoop.hbase.mapreduce.{LoadIncrementalHFiles, HFileOutputFormat2, HFileOutputFormat}

import org.apache.hadoop.hbase.{HConstants, KeyValue, HBaseConfiguration}

import org.apache.hadoop.hbase.io.ImmutableBytesWritable

import org.slf4j.{LoggerFactory, Logger}

/*
 * Mapper class - outputs rowkey and KeyValue(for delete)
 * Have included only one column family to to be deleted, but if there are more column families we need to
 * include them all for a complete row deletion
 */
class DeleteHFileMapper extends Mapper[Object, Text, ImmutableBytesWritable, KeyValue] {

  val hKey: ImmutableBytesWritable = new ImmutableBytesWritable()

  // mapper outputs key and keyValue (for Delete)
  override def map(
    idObj: Object,
    value: Text,
    context: Mapper[Object, Text, ImmutableBytesWritable, KeyValue]#Context
  ): Unit = {

    val rowKey = entityId(value.toString)
    hKey.set(rowKey)

   
    // By writing KeyVal1 in the context, it failed to generate the Hfiles saying empty string in the path
    // as it looks for column family information while writing the hfiles (outputpath/cf/....) cf is empty if we give only keyVal1 in the following code
    //val keyVal1: KeyValue = new KeyValue(rowKey, HConstants.LATEST_TIMESTAMP, KeyValue.Type.Delete)
    //context.write(hKey, keyVal1)

    // There could be more experiments possible with the KeyValue constructors to suit your need
    val keyVal2: KeyValue = new KeyValue(rowKey, utf8(Tables.party.FeaturesFamily) , null, HConstants.LATEST_TIMESTAMP, KeyValue.Type.DeleteFamily)
    context.write(hKey, keyVal2)
  }
}

object DeleteRowKeysBulkDelete {

  /** function to set the permission of an hdfs directory */
  def changePermission(dir: String,perm: String, recursive: Boolean, conf: Configuration): Int = {
    val cmdArgs: Array[String] = recursive match {
      case true  =>  Array("-chmod", "-R", perm, dir)
      case false =>  Array("-chmod", perm, dir)
    }
    val fshell = new FsShell(conf)
    fshell.run(cmdArgs)
  }

  //TBD arguments parsing using scallop
  def main (args: Array[String]): Unit = {

    val logger: Logger = LoggerFactory.getLogger(this.getClass)

    val conf       = new Configuration()
    val hbaseConf  = HBaseConfiguration.create(conf)
    val fs         = FileSystem.get(conf)
    val idFilePath = new Path(args(0))
    val hFileDir   = args(1)
    val hFilePath  = new Path(hFileDir)
    val tableName  = args(2)
    val hTable     = new HTable(hbaseConf, tableName)

    // job settings
    val job  = Job.getInstance(hbaseConf, "HBase Bulk Delete Rows")
    job.setJarByClass(DeleteRowKeysBulkDelete.getClass)
    job.setInputFormatClass(classOf[TextInputFormat])
    FileInputFormat.addInputPath(job, idFilePath)
    job.setOutputFormatClass(classOf[HFileOutputFormat]);

    // splitting the id file to spawn 2-3 mappers
    FileInputFormat.setMaxInputSplitSize(job, 80 * 1024 * 1024)

    // mapper and reducer settings
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])
    job.setOutputFormatClass(classOf[HFileOutputFormat])
    job.setMapperClass(classOf[DeleteHFileMapper])

    FileOutputFormat.setOutputPath(job, hFilePath)
    HFileOutputFormat.configureIncrementalLoad(job, new HTable(hbaseConf, tableName))

    job.waitForCompletion(true)

    /* changing the permissions of the hfiles created as bulk loader is run as hbase user, not the user who 
     * runs this job 
     */

    changePermission(hFileDir, "777", true, hbaseConf) match {
      case 0 => {
        logger.info(s"Successfully changed file permission for ${hFileDir}")
      }
      case _ => {
        logger.error(s"Unable to change permission for ${hFileDir}")
         System.exit(-1)
      }
    }

    logger.info("Calling Bulk Loader")
    val loader = new LoadIncrementalHFiles(hbaseConf)
    loader.doBulkLoad(hFilePath, hTable)
  }
}
