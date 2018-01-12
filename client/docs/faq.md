---
layout: page
title: 'RecordService FAQ'
---
**Q: Where and how are permissions managed?**

**A:** Sentry handles permissions in general, but there need to be additional queries for setting finer grained access.

In the current version of Sentry, permissions on views allow for fine-grained access control &mdash; restricting access by column and row. In a future release, we  will likely add explicit column-level permissions are likely to be added. Impala/Hive/Hue.

**Q: What kind of data can we use RecordService for?**

**A:** Hive(/Impala) tables only. Support might be added in the future for other schema sources, depending on customer demand.

**Q: What happens if you try to access data controlled by RecordService** without using RecordService?

**A:** Sentry's HDFS Sync feature ensures that the files are locked down such that only users with full access to all of the values in a file (with Sentry permissions for the entire table) are allowed to read the files directly.

**Q: Is there any API to discover the permissions that are set?** 

**A:** Hue can show this, as well as SHOW commands in Hive or Impala CLI.

**Q: What is the RecordService security model? Is it possible to purposely restrict my view into the data?**

**A:** Yes it is possible to control permissions per view. With respect to setting the active role(s), that's not something currently supported but might be supported in the future.

**Q: Why does RecordService implement its own schema (which seems to be a copy of Hive's schema)?**

**A:** The client API is layered so that it doesn't have to pull in all dependencies. As you move higher in the client API, you get access to more standard Hadoop objects. In this case, you get a recordservice-hive JAR that  returns Hive Schema objects. 

**Q: Is there a way to list tables through RecordService, or do we go through the Hive metastore to get tables, then ask RecordService for the schema?**

**A:** Currently, you can't list tables through RecordService. You have to go through the Hive metastore. 

Also note that the only thing needed to read from a table is the fully qualified table name, so the client does not need to pass the table metadata to RS.

**Q: How does accessing a path directly versus querying the Hive metastore work?**

**A:** RecordService infers the schema to the best of its ability. If the path contains a self-describing file, such as Avro or Parquet, it will use that. For files like CSV, RecordService defaults to a STRING schema. The security rules for paths are still under consideration. For now, RecordServices does not infer schema &mdash; it only deals with tables defined in the Hive metastore.
