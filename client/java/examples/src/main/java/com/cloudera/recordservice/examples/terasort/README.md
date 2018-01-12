This is a modification of terasort from the hadoop examples. There are two modifications:

1. The data gen is change slightly. The standard data looks like:
   (10 bytes key) (constant 2 bytes) (32 bytes rowid)
   (constant 4 bytes) (48 bytes filler) (constant 4 bytes)
   The rowid is the right justified row id as a hex number.

   We modify it so that after each record ends with '\n' and ensure that '\n' does not show up
   any where else in the record. This makes the data more tabular and the a table with 1 string
   column can be specified over the data. This effectively makes the records have pay load of
   89 bytes instead of 90.

2. Terasort uses a custom input format. We've added a [RecordService compatible input
   format](RecordServiceTeraInputFormat.java) as well.

**Steps**:

1. Generate data source using [TeraGen](TeraGen.java):
   ```bash
   hadoop jar recordservice-examples-${version}.jar \
   com.cloudera.recordservice.examples.terasort.TeraGen <num_records> <out_dir>
   ```
   *Note: the parallelism is determined by `mapreduce.job.maps` which defaults to a low value.
   This should be set much higher, ~1 per core per node.*

2. Create a HMS table with generated directory in step 1). This is used for RecordService.
   ```bash
   impala-shell -q "create external table <table>(record STRING) location 'out_dir'"
   ```

3. Make sure the result table has the expected number of records.
   ```bash
   impala-shell -q "select count(*) from <table>".
   ```

4. Run the standard [TeraChecksum](TeraChecksum.java) without RecordService.
   ```bash
   hadoop jar recordservice-examples-${version}.jar \
   com.cloudera.recordservice.examples.terasort.TeraChecksum <out_dir> <tmp_dir>
   ```

5. Run the [TeraChecksum](TeraChecksum.java) using RecordService.
   ```bash
   hadoop jar recordservice-examples-${version}.jar \
   com.cloudera.recordservice.examples.terasort.TeraChecksum <table> <tmp_dir2> true
   ```

6. Make sure the results from step 4) and step 5) are the same
   ```bash
   hadoop fs -cat <tmp_dir1>/part-r-00000
   hadoop fs -cat <tmp_dir2>/part-r-00000
   ```

7. Run [TeraSort](TeraSort.java)
   ```bash
   hadoop jar recordservice-examples-${version}.jar \
   com.cloudera.recordservice.examples.terasort.TeraSort <out_dir> <sorted_dir1>
   hadoop jar recordservice-examples-${version}.jar \
   com.cloudera.recordservice.examples.terasort.TeraSort <table> <sorted_dir2> true
   ```

8. Run [TeraValidate](TeraValidate)
   ```bash
   impala-shell -q "create external table <sorted_table>(record STRING) location '<sorted_dir2>'"
   hadoop jar recordservice-examples-${version}.jar \
   com.cloudera.recordservice.examples.terasort.TeraValidate <sorted_dir1> <validate_dir1>
   hadoop jar recordservice-examples-${version}.jar \
   com.cloudera.recordservice.examples.terasort.TeraValidate <sorted_table> <validate_dir2> true
   ```

9. Make sure results from step 8) for both w/ RS and w/o RS are the same.
   ```bash
   hadoop fs -cat <validate_dir1>/part-r-00000
   hadoop fs -cat <validate_dir2>/part-r-00000
   ```

**Example**:

First clear some paths:

```bash
hadoop fs -rm -r /tmp/terasort /tmp/checksum1 /tmp/checksum2 \
/tmp/sorted1 /tmp/sorted2 /tmp/validate1 /tmp/validate2
impala-shell -q "drop table if exists terasort_data"
impala-shell -q "drop table if exists sorted_terasort_data"
```

1. Generate the test file
   ```bash
   hadoop jar recordservice-examples-${version}.jar \
   com.cloudera.recordservice.examples.terasort.TeraGen 10 /tmp/terasort
   ```

2. Create the test table using the test file
   ```bash
   impala-shell -q "create external table terasort_data(record STRING) location '/tmp/terasort'"
   ```

3. Make sure the test table has the expected number of records
   ```bash
   impala-shell -q "select count(*) from terasort_data"
   ```

4. Run [TeraChecksum](TeraChecksum.java) without RecordService
   ```bash
   hadoop jar recordservice-examples-${version}.jar \
   com.cloudera.recordservice.examples.terasort.TeraChecksum /tmp/terasort /tmp/checksum1
   ```

5. Run [TeraChecksum](TeraChecksum.java) with RecordService
   ```bash
   hadoop jar recordservice-examples-${version}.jar \
   com.cloudera.recordservice.examples.terasort.TeraChecksum "terasort_data" /tmp/checksum2 true
   ```

6. Make sure the results from step 4) and step 5) are the same
   ```bash
   hadoop fs -cat /tmp/checksum1/part-r-00000
   hadoop fs -cat /tmp/checksum2/part-r-00000
   ```

7. Run [TeraSort](TeraSort.java) without or with RecordService
   - Without RecordService:
   ```bash
   hadoop jar recordservice-examples-${version}.jar \
   com.cloudera.recordservice.examples.terasort.TeraSort /tmp/terasort /tmp/sorted1
   ```

   - With RecordService
   ```bash
   hadoop jar recordservice-examples-${version}.jar \
   com.cloudera.recordservice.examples.terasort.TeraSort terasort_data /tmp/sorted2 true
   ```

8. Run [TeraValidate](TeraValidate) without or with RecordService
   - Without RecordService
   ```bash
   hadoop jar recordservice-examples-${version}.jar \
   com.cloudera.recordservice.examples.terasort.TeraValidate /tmp/sorted1 /tmp/validate1
   ```

   - With RecordService
   ```bash
   impala-shell -q "create external table sorted_terasort_data(record STRING) location '/tmp/sorted2'"
   hadoop jar recordservice-examples-${version}.jar \
   com.cloudera.recordservice.examples.terasort.TeraValidate \
   sorted_terasort_data /tmp/validate2 true
   ```

9. Make sure results from step 8) for both w/ RS and w/o RS are the same.
   ```bash
   hadoop fs -cat /tmp/validate1/part-r-00000
   hadoop fs -cat /tmp/validate2/part-r-00000
   ```
