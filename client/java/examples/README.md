## What's in this package

This repo contains examples of applications built using RecordService client APIs.

- `RSCat`: output tabular data for any data set readable by RecordService

- `SumQueryBenchmark`: This is an example of running a simple sum over a column,
  pushing the scan to RecordService.

- `Terasort`: terasort ported to RecordService. See README in package for more
  details. This also demonstrates how to implement a custom InputFormat using
  the RecordService APIs.

- `MapredColorCount`/`MapreduceAgeCount`/`MapReduceColorCount`: These are the examples
  ported from Apache Avro and demonstrate the steps required to port an existing
  Avro-based MapReduce job to use RecordService.

- `RecordCount`/`Wordcount`: More simple MapReduce applications that demonstrate some
  of the other InputFormats that are included in the client library.

- `com.cloudera.recordservice.examples.avro`: Unmodified from the [Apache Avro](https://avro.apache.org/) examples.
  We've included these to simplify sample data generation.

## How to enforce Sentry permissions with MapReduce

With RecordService, MapReduce users can now enforce restrictions on data using Sentry
privileges and provide fine grain (row and column-level) authorization using Hive Metastore
views. Here we show two simple examples on how to do this: one using **SQL query**, and
another using **path request**. We also have examples showing how to do this in Spark. Please
check the [Spark example](../examples-spark/README.md#how-to-enforce-sentry-permissions-with-spark) for details.

Before jumping into the examples, please make sure the Sentry Service is started.
If you're using the [QuickStart VM](https://github.com/cloudera/recordservice-quickstart) provided
by us, this can simply be done by:

```bash
sudo service sentry-store restart
```

### Reading data through SQL query

If the data is a registered table in Hive MetaStore, currently there are two ways to enforce
fine-grained column-level privilege for the table: using column-level security
, which is a new feature starting from Sentry 1.5, or creating a view on the selected columns of the table.
In the following we'll describe these two approaches separately.

First, for demonstration purpose, we shall create a test group and add the current user to that group:

```bash
sudo groupadd demo_group
sudo usermod -a -G demo_group $USER
```

Then, creating a test role called `demo_role`, and add it to the group `demo_group` defined above.
This can be done with either Impala or Hive (we use Impala here).

```bash
sudo -u impala impala-shell
[quickstart.cloudera:21000] > CREATE ROLE demo_role;
[quickstart.cloudera:21000] > GRANT ROLE demo_role to GROUP demo_group;
```

We shall use `tpch.nation` as example, which is also available in our QuickStart VM.
The schema for this table looks like:

| column_name | column_type |
|-------------|-------------|
| n_nationkey | smallint    |
| n_name      | string      |
| n_regionkey | smallint    |
| n_comment   | string      |

#### Granting permissions using column-level security

Suppose we want some users with a particular role to only be able to read the
`n_nationkey` and `n_name` columns, we can do the following:

```bash
sudo -u impala impala-shell
[quickstart.cloudera:21000] > GRANT SELECT(n_name, n_nationkey) ON TABLE tpch.nation TO ROLE demo_role;
```

This grants the **SELECT** privilege on columns `n_name` and `n_nationkey` to the role `demo_role`.

#### Running MR Job

Now, if we want to count the number of records in the `tpch.nation` with the above settings,
we can launch a MR job for [RecordCount](src/main/java/com/cloudera/recordservice/examples/mapreduce/RecordCount.java) on the table:

```bash
hadoop jar /path/to/recordservice-examples-${VERSION_NUMBER}.jar \
  com.cloudera.recordservice.examples.mapreduce.RecordCount \
  "SELECT * FROM tpch.nation" \
  "/tmp/recordcount_output"
```

It will quickly fail with this exception:

```
TRecordServiceException(code:INVALID_REQUEST, message:Could not plan request.,
detail:AuthorizationException: User 'cloudera' does not have privileges to execute 'SELECT' on:
tpch.nation)
```

Now try to just select the column which we've been granted privilege to access:

```bash
hadoop jar /path/to/recordservice-examples-${VERSION_NUMBER}.jar \
  com.cloudera.recordservice.examples.mapreduce.RecordCount \
  "SELECT n_name, n_nationkey FROM tpch.nation" \
  "/tmp/recordcount_output"
```

This will succeed with correct result.

#### Granting permission to view

Another way to enforce column level restrictions is to use views. Similar to the above,
suppose we want some users with a particular role to only be able to read the
`n_nationkey` and `n_name` columns, we can do the following:

```bash
sudo -u impala impala-shell
[quickstart.cloudera:21000] > USE tpch;
[quickstart.cloudera:21000] > CREATE VIEW nation_names AS SELECT n_nationkey, n_name FROM tpch.nation;
[quickstart.cloudera:21000] > GRANT SELECT ON TABLE tpch.nation_names TO ROLE demo_role;
```

which creates a view on the `n_nationkey`
and `n_name` columns of the `tpch.nation` table, and grant the **SELECT** privilege to
the `demo_role`.

#### Running MR Job

Now try to access the `tpch.nation_names` view:

```bash
hadoop jar /path/to/recordservice-examples-${VERSION_NUMBER}.jar \
  com.cloudera.recordservice.examples.mapreduce.RecordCount \
  "SELECT * FROM tpch.nation_names" \
  "/tmp/recordcount_output"
```

It will succeed with the correct result.

### Reading data through path request

Data can also be ready directly via **path request**, rather than specifying a projection
on a table. This is useful for reading data that is not backed by a Hive Metastore tables
as well as a way to expedite migration of existing applications which directly read files
to RecordService.

Path request requires a few different settings SQL query request. To demonstrate, we use
the path ``/test-warehouse/tpch.nation`` for the `tpch.nation` table as example.

#### Granting permission to path

First, make sure that ``sentry.hive.server`` is properly set in the ``sentry-site.xml``.

Similar to the SQL query example, we first need to grant privilege (please note that at
the moment it requires **ALL** privilege) to the chosen path. In addition, **for path
request the user who's running the ``recordserviced`` process needs to have the privilege
to create databases**. This is because internally RecordService needs to create a
temporary database and table for the path.

Assume the RecordService Planner is running as a user in the 'recordservice' group:

```bash
sudo -u impala impala-shell
[quickstart.cloudera:21000] > GRANT ALL ON URI 'hdfs:/test-warehouse/tpch.nation' TO ROLE demo_role;
[quickstart.cloudera:21000] > CREATE ROLE rs_global_admin;
[quickstart.cloudera:21000] > GRANT ROLE rs_global_admin TO GROUP recordservice;
[quickstart.cloudera:21000] > GRANT ALL ON SERVER TO ROLE rs_global_admin;
```

#### Running MR Job

Now we can access the path through RecordService. Here we use
[WordCount](src/main/java/com/cloudera/recordservice/examples/mapreduce/WordCount.java),
which counts the total number of words in all files under the path.

**Note**, you may also want to set `HADOOP_CONF_DIR` environment variable before running
the job, to specify properties for the job (e.g., RecordServicePlanner host & port,
Kerberos principal, etc). If the cluster is deployed using Cloudera Manager, then you
can deploy the client configuration first, after which it will generate a configuration
file under `/etc/recordservice/conf`.

```bash
hadoop jar /path/to/recordservice-examples-${VERSION_NUMBER}.jar \
  com.cloudera.recordservice.examples.mapreduce.WordCount \
  "/test-warehouse/tpch.nation" \
  "/tmp/wordcount_output"
```
