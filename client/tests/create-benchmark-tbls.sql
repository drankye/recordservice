-- Create databases
CREATE DATABASE IF NOT EXISTS tpch6gb;
CREATE DATABASE IF NOT EXISTS tpch6gb_parquet;
CREATE DATABASE IF NOT EXISTS tpch6gb_avro_snap;
CREATE DATABASE IF NOT EXISTS rs;

-- Create tables
DROP TABLE IF EXISTS tpch6gb.lineitem;
CREATE EXTERNAL TABLE tpch6gb.lineitem(
  l_orderkey BIGINT,
  l_partkey BIGINT,
  l_suppkey BIGINT,
  l_linenumber INT,
  l_quantity DECIMAL(12,2),
  l_extendedprice DECIMAL(12,2),
  l_discount DECIMAL(12,2),
  l_tax DECIMAL(12,2),
  l_returnflag STRING,
  l_linestatus STRING,
  l_shipdate STRING,
  l_commitdate STRING,
  l_receiptdate STRING,
  l_shipinstruct STRING,
  l_shipmode STRING,
  l_comment STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '/test-warehouse/tpch6gb.lineitem';

DROP TABLE IF EXISTS tpch6gb_parquet.lineitem;
CREATE EXTERNAL TABLE tpch6gb_parquet.lineitem like tpch6gb.lineitem
STORED AS PARQUET
LOCATION '/test-warehouse/tpch6gb_parquet.lineitem';

DROP TABLE IF EXISTS tpch6gb_avro_snap.lineitem;
CREATE EXTERNAL TABLE tpch6gb_avro_snap.lineitem
STORED AS AVRO
LOCATION '/test-warehouse/tpch6gb_avro_snap.lineitem'
TBLPROPERTIES (
    'avro.schema.literal'='{
"fields": [{"type": ["long", "null"], "name": "L_ORDERKEY"}, {"type": ["long", "null"], "name": "L_PARTKEY"}, {"type": ["long", "null"], "name": "L_SUPPKEY"}, {"type": ["int", "null"], "name": "L_LINENUMBER"}, {"type": [{"logicalType": "decimal", "scale": 2, "type": "bytes", "precision": 12}, "null"], "name": "L_QUANTITY"}, {"type": [{"logicalType": "decimal", "scale": 2, "type": "bytes", "precision": 12}, "null"], "name": "L_EXTENDEDPRICE"}, {"type": [{"logicalType": "decimal", "scale": 2, "type": "bytes", "precision": 12}, "null"], "name": "L_DISCOUNT"}, {"type": [{"logicalType": "decimal", "scale": 2, "type": "bytes", "precision": 12}, "null"], "name": "L_TAX"}, {"type": ["string", "null"], "name": "L_RETURNFLAG"}, {"type": ["string", "null"], "name": "L_LINESTATUS"}, {"type": ["string", "null"], "name": "L_SHIPDATE"}, {"type": ["string", "null"], "name": "L_COMMITDATE"}, {"type": ["string", "null"], "name": "L_RECEIPTDATE"}, {"type": ["string", "null"], "name": "L_SHIPINSTRUCT"}, {"type": ["string", "null"], "name": "L_SHIPMODE"}, {"type": ["string", "null"], "name": "L_COMMENT"}], "type": "record", "name": "a"
    }');

-- Table that is used for Hive tests and benchmarks. Until Hive has transparent support
-- for RecordService SerDe, queries are executed against this table and then redirected
-- to run against the actual db/table.
DROP TABLE IF EXISTS rs.lineitem_hive_serde;
CREATE EXTERNAL TABLE rs.lineitem_hive_serde like tpch6gb.lineitem
STORED BY RECORDSERVICE;
