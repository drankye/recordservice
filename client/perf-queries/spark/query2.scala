# Spark w/ Text
var tpch = sc.textFile("hdfs://localhost:20500/test-warehouse/tpch10gb.db/lineitem/*")
var q2 = tpch.map(line => line.split('|')(15)).reduce((x,y) => if (x < y) x else y)

# Spark SQL w/ Parquet
var path = "hdfs://localhost:20500/test-warehouse/tpch10gb_parquet.db/lineitem/*"
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val data = sqlContext.parquetFile(path)
data.registerTempTable("lineitem")
sqlContext.sql("select min(l_comment) from lineitem").collect()

# Spark SQL w/ Avro:
bin/spark-shell --jars <path to> avro-1.7.8-SNAPSHOT.jar,avro-mapred-1.7.8-SNAPSHOT.jar,spark-avro_2.10-0.1.jar
import org.apache.spark.sql.SQLContext
val sqlContext = new SQLContext(sc)
import sqlContext._
import com.databricks.spark.avro._
val data = sqlContext.avroFile("/test-warehouse/tpch10gb_avro_snap.db/lineitem/*")
data.registerTempTable("lineitem")
sqlContext.sql("select min(L_COMMENT) from lineitem").collect()

# With RS
import com.cloudera.recordservice.spark._
val data = sc.recordServiceRecords("select l_comment from tpch10gb.lineitem").
    map(v => v(0).asInstanceOf[org.apache.hadoop.io.Text].toString()).
    reduce( (x,y) => if (x < y) x else y)
