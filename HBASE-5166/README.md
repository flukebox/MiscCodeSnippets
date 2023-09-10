#HBASE-5166

https://issues.apache.org/jira/browse/HBASE-5166


There is no MultiThreadedTableMapper in hbase currently just like we have a MultiThreadedMapper in Hadoop for IO Bound Jobs.
UseCase, webcrawler: take input (urls) from a hbase table and put the content (urls, content) back into hbase.
Running these kind of hbase mapreduce job with normal table mapper is quite slow as we are not utilizing CPU fully (N/W IO Bound).

Moreover, I want to know whether It would be a good/bad idea to use HBase for these kind of usecases ?.



