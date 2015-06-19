# hbase-utils-scala
Some hbase utils which could be useful, written in Scala

`Build Settings`

Build settings in this repo are not complete, as I moved this code from our internal repo and it refers to our internal
artifiactory. You might have to add your own dependency settings.

`Delete Utils`
============
There are three options being added for now. I got an HDFS file with a given set of Ids (Strings) which need to be
removed/deleted from an HBase table.

> Option 1
  Delete the given set of Ids using a client job. This could be useful for a minimal number of Ids (not for bulk).
  I could not delete more than 10000 in a single run as the regions timed out quickly. (A healthy cluster could do more)

> Option 2
  Spawn a mapreduce job (Map Only) and do parrallel RPC calls (for Delete) from  different mappers.
  This was another experiment, but same result as Option 1.
 
> Option 3
  Generate HFiles (with Delete marker) and do a bulk load. This solution worked for my case as I had around
  10 million ids to be deleted. The job gets completed quickly (in 3-5 minutes) compared to previous options,
  but the table was not responding to a scan/get for a longer time after this job. It is advisable to perform
  this in batches (In my case I am going to split 10 million into a number of batches and runs)
 
