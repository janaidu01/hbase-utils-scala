# hbase-utils-scala
Some hbase utils which could be useful, written in Scala

`Build Settings`

Build settings in this repo are not complete, as I moved this code from our internal repo and it refers to our internal
artifiactory. You might have to add your own dependency settings.

>`Delete Utils`
============
There are three options being added for now. I got an HDFS file with a given set of Ids (Strings) which need to be
removed/deleted from an HBase table.

Option 1
  Delete the given set of Ids using a client job.

Option 2
  Spawn a mapreduce job (Map Only) and do parrallel RPC calls (for Delete) from  different mappers
  
Option 3
  Generate HFiles (with Delete marker) and do a bulk load
 
