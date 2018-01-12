-- Schema for mysql performance database
create table perf_db(
  date datetime,
  build int,
  version varchar(100),
  workload varchar(100),
  client varchar(100),
  runtime_ms float,
  labels varchar(2000));



