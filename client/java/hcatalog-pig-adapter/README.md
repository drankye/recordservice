## Using HCatalog/Pig with RecordService

This package and the package [hcatalog](../hcatalog) add RecordService support
for HCatalog/Pig applications. In the following we describe how to enable this feature.

Let's take the following pig script as an example:

```bash
A = LOAD 'tpch.nation' USING org.apache.hive.hcatalog.pig.HCatLoader();
DUMP A;
```

To enable RecordService, you simply need to add a few lines in the script:

```bash
register /path/to/recordservice-hcatalog-pig-adapter-${version}-jar-with-dependencies.jar
set recordservice.planner.hostports <planner-hostports>
A = LOAD 'tpch.nation' USING com.cloudera.recordservice.pig.HCatRSLoader();
DUMP A;
```

Note the changes:

1. You need to register the jar file that contains all the necessary classes, including
   the `HCatRSLoader` class, that are required to use HCatalog/Pig with RecordService.

2. You need to specify the address for RecordService planners. We can either do this by
   specifying the `recordservice.planner.hostports` property, or by specifying the
   `recordservice.zookeeper.connectString` to use the planner auto discovery feature.

3. You need to replace the original `HCatLoader` with the `HCatRSLoader`.

Additional RecordService properties can be specified in similar way using the `set` command.

For example, in a cluster with kerberos, you need to set kerberos principal in the script:
```bash
set recordservice.kerberos.principal ${primary}/_HOST@${REALM}
```

Besides, RecordService supports column projection:

```bash
register /path/to/recordservice-hcatalog-pig-adapter-${version}-jar-with-dependencies.jar
set recordservice.planner.hostports <planner-hostports>
A = LOAD 'select n_nationkey, n_name from tpch.nation' USING com.cloudera.recordservice.pig.HCatRSLoader();
DUMP A;
```

This will only select the column `n_nationkey` and `n_name` from the `tpch.nation` table.
Similar to MapReduce or Spark, user can enforce restrictions on data access using Sentry
privileges, which also applies to the HCatalog/Pig jobs.
