-- Create databases
-- functional is needed for Hive serde test
-- on the server side. The test itself assumes the existence
-- of this database and creates table inside it. Besides this,
-- we don't need any table from this database at the moment.
CREATE DATABASE IF NOT EXISTS functional;
CREATE DATABASE IF NOT EXISTS tpch;
CREATE DATABASE IF NOT EXISTS rs;

-- Create tables
DROP TABLE IF EXISTS tpch.nation;
CREATE EXTERNAL TABLE tpch.nation (
  N_NATIONKEY SMALLINT,
  N_NAME STRING,
  N_REGIONKEY SMALLINT,
  N_COMMENT STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '/test-warehouse/tpch.nation';

-- Create a view that is a projection of tpch.nation
DROP VIEW IF EXISTS rs.nation_projection;
CREATE VIEW rs.nation_projection AS
SELECT n_nationkey, n_name from tpch.nation where n_nationkey < 5;

-- Create all types table.
DROP TABLE IF EXISTS rs.alltypes;
CREATE TABLE rs.alltypes(
  bool_col BOOLEAN,
  tinyint_col TINYINT,
  smallint_col SMALLINT,
  int_col INT,
  bigint_col BIGINT,
  float_col FLOAT,
  double_col DOUBLE,
  string_col STRING,
  varchar_col VARCHAR(10),
  char_col CHAR(5),
  timestamp_col TIMESTAMP,
  decimal_col decimal(24, 10))
STORED AS TEXTFILE;

DROP TABLE IF EXISTS rs.alltypes_null;
CREATE TABLE rs.alltypes_null like rs.alltypes;

DROP TABLE IF EXISTS rs.alltypes_empty;
CREATE TABLE rs.alltypes_empty like rs.alltypes;

-- Populate the table with two inserts, this creates two files/two blocks.
insert overwrite rs.alltypes VALUES(true, 0, 1, 2, 3, 4.0, 5.0, "hello",
  cast("vchar1" as VARCHAR(10)),
  cast("char1" as CHAR(5)),
  "2015-01-01",
  3.141592);
insert into rs.alltypes VALUES(false, 6, 7, 8, 9, 10.0, 11.0, "world",
  cast("vchar2" as VARCHAR(10)),
  cast("char2" as CHAR(5)),
  "2016-01-01",
  1234.567890);

insert overwrite rs.alltypes_null VALUES(
  NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

-- Create users table and insert values.
DROP TABLE IF EXISTS rs.users;
create table rs.users (name STRING, favorite_number INT, age INT, favorite_animal STRING, favorite_color STRING);

INSERT OVERWRITE TABLE rs.users VALUES ("Alyssa", 256, 10, "sun bear", "red");
INSERT INTO TABLE rs.users VALUES ("Ben", 96, 21, "dog", "blue");
INSERT INTO TABLE rs.users VALUES ("Kate", 51, 14, "elk", "blue");
INSERT INTO TABLE rs.users VALUES ("Laura", 19, 30, "cat", "red");
INSERT INTO TABLE rs.users VALUES ("Mike", 22, 60, NULL, NULL);
INSERT INTO TABLE rs.users VALUES ("Zack", 1, 56, "dog", "red");

-- Create nullUsers table and insert values.
DROP TABLE IF EXISTS rs.nullUsers;
create table rs.nullUsers (name STRING, favorite_number INT, age INT, favorite_animal STRING, favorite_color STRING);

INSERT OVERWRITE TABLE rs.nullUsers VALUES ("Alyssa", 256, NULL, "sun bear", "red");
