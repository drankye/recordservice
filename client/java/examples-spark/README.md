## What's in this package

This repo contains Spark examples of applications built using RecordService client APIs.

- `Query1`/`Query2`: simple examples demonstrate how to use `RecordServiceRDD` to query
  a Parquet table.

- `WordCount`: a Spark application that counts the words in a directory. This demonstrates
  how to use the `SparkContext.recordServiceTextfile`.

- `SchemaRDDExample`: a example demonstrate how to use `RecordServiceSchemaRDD` to query tables.

- `TeraChecksum`: a terasort example that ported to Spark. It can be run with or without
  RecordService.

- `TpcdsBenchmark`: driver that can be used to run [TPC-DS](http://www.tpc.org/tpcds/) benchmark
  queries. It shows how to use SparkSQL and RecordService to execute the queries.

- `DataFrameExample`: a simple example demonstrate how to use DataFrame with RecordService by
  setting the data source.

- `RecordCount`: a simple Spark app that is similar to the MapReduce version of `RecordCount`.


## How to use RecordService with Spark shell

To use Spark shell with RecordService, first you'll need to start up `spark-shell`
with RecordService jar:

```bash
spark-shell --properties-file=/path/to/properties-file --jars /path/to/recordservice-spark.jar
```

where the parameter `properties-file` should include all the necessary properties. For instance:

```
spark.recordservice.planner.rpc.timeoutMs=-1
spark.recordservice.worker.rpc.timeoutMs=-1
spark.recordservice.worker.server.enableLogging=false
spark.recordservice.kerberos.principal= primary/instance@REALM
spark.recordservice.planner.hostports=plannerHost:port
```

If the cluster is deployed with Cloudera Manager, then you'll need to deploy client configuration
first. After this you can use the generated `spark.conf` under `/etc/recordservice/conf`.

Then follow the examples to use Spark shell with RecordService in different ways.
For clarity most of the log outputs are omitted from the output, also the output may
vary depending on your environment, Spark version, etc.

- Example on using `RecordServiceRDD`

```scala
scala> import com.cloudera.recordservice.spark._
import com.cloudera.recordservice.spark._

scala> val data = sc.recordServiceRecords("select * from tpch.nation")
data: org.apache.spark.rdd.RDD[Array[org.apache.hadoop.io.Writable]] = RecordServiceRDD[0] at RDD at RecordServiceRDDBase.scala:57

scala> data.count()
res0: Long = 25
```

- Example on using DataFrame with RecordService

```scala
scala> import com.cloudera.recordservice.spark._
import com.cloudera.recordservice.spark._

scala> val context = new org.apache.spark.sql.SQLContext(sc)
context: org.apache.spark.sql.SQLContext = org.apache.spark.sql.SQLContext@706ef676

scala> val df = context.load("tpch.nation", "com.cloudera.recordservice.spark")
df: org.apache.spark.sql.DataFrame = [n_nationkey: int, n_name: string, n_regionkey: int, n_comment: string]

scala> val results = df.groupBy("n_regionkey").count().orderBy("n_regionkey").collect()
results: Array[org.apache.spark.sql.Row] = Array([0,5], [1,5], [2,5], [3,5], [4,5])
```

- Example on using SparkSQL with RecordService

```scala
scala> val context = new org.apache.spark.sql.SQLContext(sc)
context: org.apache.spark.sql.SQLContext = org.apache.spark.sql.SQLContext@53bfb38e

scala> context.sql(s"""
     | CREATE TEMPORARY TABLE nationTbl
     | USING com.cloudera.recordservice.spark
     | OPTIONS (
     |   RecordServiceTable 'tpch.nation'
     | )
     | """)
res0: org.apache.spark.sql.DataFrame = []

scala> val result = context.sql("SELECT count(*) FROM nationTbl").collect()(0).getLong(0)
result: Long = 25
```

## How to enforce Sentry permissions with Spark

With RecordService, Spark users can now enforce restrictions on data with **views**.
For details on how to create and set up access to views, please check out the [MapReduce example](../examples/README.md#how-to-enforce-sentry-permissions-with-mapreduce).

### Reading data through SQL query

Similarly to MapReduce, if the data is a table in Hive MetaStore, one can get
fine-grained access to it through either the column-level security feature starting from
Sentry 1.5, or by creating a separate **view** with selected columns on the original table.
Following the MapReduce example, suppose we only want to give the role `demo_role` access to
the columns `n_name` and `n_nationkey`, and suppose both the column privileges and the view
are already set up, now if we launch a Spark job for [RecordCount](src/main/scala/com/cloudera/recordservice/examples/spark/RecordCount.scala)
on the `tpch.nation` table mentioned in the MapReduce example:

```
spark-submit \
  --class com.cloudera.recordservice.examples.spark.RecordCount \
  --master <master-url> \
  --properties-file /path/to/properties-file \
  /path/to/recordservice-examples-spark-${VERSION_NUMBER}.jar \
  "SELECT * FROM tpch.nation"
```

The job will fail with this exception:

```
TRecordServiceException(code:INVALID_REQUEST, message:Could not plan request.,
detail:AuthorizationException: User 'cloudera' does not have privileges to execute 'SELECT' on:
tpch.nation)
```
However, accessing the columns we've been granted privileges with will be OK:

```
spark-submit \
  --class com.cloudera.recordservice.examples.spark.RecordCount \
  --master <master-url> \
  --properties-file /path/to/properties-file \
  /path/to/recordservice-examples-spark-${VERSION_NUMBER}.jar \
  "SELECT n_name, n_nationkey FROM tpch.nation"

```

Accessing the `tpch.nation_names` view is also OK:

```
spark-submit \
  --class com.cloudera.recordservice.examples.spark.RecordCount \
  --master <master-url> \
  --properties-file /path/to/properties-file \
  /path/to/recordservice-examples-spark-${VERSION_NUMBER}.jar \
  "SELECT * FROM tpch.nation_names"

```

### Reading data through path request

Like the MapReduce example, one can also use path request to read data
that is not registered in Hive MetaStore. Here we use
[WordCount](src/main/scala/com/cloudera/recordservice/examples/spark/WordCount.scala)
as example:

```
spark-submit \
  --class com.cloudera.recordservice.examples.spark.WordCount \
  --master <master-url> \
  --properties-file /path/to/properties-file \
  /path/to/recordservice-examples-spark-${VERSION_NUMBER}.jar
```
