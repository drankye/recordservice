# With RS
import com.cloudera.recordservice.spark._
val data = sc.recordServiceRecords("select l_partkey from tpch10gb_parquet.lineitem").
    map(v => v(0).asInstanceOf[org.apache.hadoop.io.LongWritable].get()).
    reduce(_ + _)

# Run w/ Text:
var path = "hdfs://localhost:20500/test-warehouse/tpch10gb.db/lineitem/*"
var path = "hdfs://localhost:20500/test-warehouse/tpch.lineitem/*"

var lineitem = sc.textFile(path)
lineitem.map(line => line.split('|')(1).toLong).reduce(_ + _)

# Run w/ Parquet:
var path = "hdfs://localhost:20500/test-warehouse/tpch10gb_parquet.db/lineitem/*"
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val data = sqlContext.parquetFile(path)
data.registerTempTable("lineitem")
sqlContext.sql("select sum(l_partkey) from lineitem").collect()

# Run w/ Avro (very painful):
# Get and build spark-avro (patched)
# https://github.com/nongli/spark-avro/tree/glob_fix
# Get and build avro 1.7.8

bin/spark-shell --jars <path to> avro-1.7.8-SNAPSHOT.jar,avro-mapred-1.7.8-SNAPSHOT.jar,spark-avro_2.10-0.1.jar
import org.apache.spark.sql.SQLContext
val sqlContext = new SQLContext(sc)
import sqlContext._
import com.databricks.spark.avro._
val data = sqlContext.avroFile("/test-warehouse/tpch10gb_avro_snap.db/lineitem/*")

# Note: spark sql appears to be case sensitive on identifiers
data.registerTempTable("lineitem")
sqlContext.sql("select sum(L_PARTKEY) from lineitem").collect()

