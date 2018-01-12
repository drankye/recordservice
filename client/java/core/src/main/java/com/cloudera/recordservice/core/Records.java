// Copyright 2014 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.recordservice.core;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;

import com.cloudera.recordservice.core.Schema.Type;
import com.cloudera.recordservice.util.Preconditions;

import sun.misc.Unsafe;

/**
 * Abstraction over records returned from the RecordService. This class is
 * extremely performance sensitive. Not thread-safe.
 */
@SuppressWarnings("restriction")
public class Records implements Closeable {
  private static final Unsafe unsafe;
  static {
    try {
      Field field = Unsafe.class.getDeclaredField("theUnsafe");
      field.setAccessible(true);
      unsafe = (Unsafe) field.get(null);
    } catch (NoSuchFieldException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Representation of a single record, providing accessors to get each
   * column by index.
   */
  public static final class Record {
    // For each column, the current offset to return (column data is sparse)
    private final long[] colOffsets_;

    // For each column, the serialized data values.
    private final ByteBuffer[] colData_;
    private final ByteBuffer[] nulls_;

    // Only used if col[i] is a String to prevent object creation.
    private final ByteArray[] byteArrayVals_;

    // Only used if col[i] is a TimestampNano to prevent object creation.
    private final TimestampNanos[] timestampNanos_;

    // Only used if col[i] is a Decimal to prevent object creation.
    private final Decimal[] decimalVals_;

    // Current record idx (in the current batch) being returned
    private int recordIdx_;

    // Total number of records in the current batch.
    private long numRecords_;

    // If the type is a CHAR or DECIMAL, this contains the length of the value.
    // -1 otherwise.
    private final int[] byteArrayLen_;

    // Schema of the record
    private final Schema schema_;

    // Returns if the col at 'colIdx' is NULL.
    public boolean isNull(int colIdx) {
      if (schema_.isCountStar) return true;
      return nulls_[colIdx].get(recordIdx_) != 0;
    }

    private static final long byteArrayOffset = unsafe.arrayBaseOffset(byte[].class);

    /**
     * For all these getters, returns the value at 'colIdx' and advances to the next
     * value. This must be called in lock-step with Records.next(). This is optimized
     * for the common case where the caller will read every value of every record.
     * It is invalid to call next*() for a subset of the records.
     * The type of the column must match. Undefined behavior if it does not match
     * or if called on a value that is NULL. (We don't want to verify because this
     * is the hot path.)
     * TODO: add a DEBUG mode which verifies the API is being used correctly.
     */
    public final boolean nextBoolean(int colIdx) {
      byte val = unsafe.getByte(colData_[colIdx].array(), colOffsets_[colIdx]);
      colOffsets_[colIdx] += 1;
      return val != 0;
    }

    public final byte nextByte(int colIdx) {
      byte val = unsafe.getByte(colData_[colIdx].array(), colOffsets_[colIdx]);
      colOffsets_[colIdx] += 1;
      return val;
    }

    public final short nextShort(int colIdx) {
      short val = unsafe
          .getShort(colData_[colIdx].array(), colOffsets_[colIdx]);
      colOffsets_[colIdx] += 2;
      return val;
    }

    public final int nextInt(int colIdx) {
      int val = unsafe.getInt(colData_[colIdx].array(), colOffsets_[colIdx]);
      colOffsets_[colIdx] += 4;
      return val;
    }

    public final long nextLong(int colIdx) {
      long val = unsafe.getLong(colData_[colIdx].array(), colOffsets_[colIdx]);
      colOffsets_[colIdx] += 8;
      return val;
    }

    public final float nextFloat(int colIdx) {
      float val = unsafe.getFloat(colData_[colIdx].array(), colOffsets_[colIdx]);
      colOffsets_[colIdx] += 4;
      return val;
    }

    public final double nextDouble(int colIdx) {
      double val = unsafe.getDouble(colData_[colIdx].array(), colOffsets_[colIdx]);
      colOffsets_[colIdx] += 8;
      return val;
    }

    public final ByteArray nextByteArray(int colIdx) {
      int len = byteArrayLen_[colIdx];
      if (len < 0) {
        len = unsafe.getInt(colData_[colIdx].array(), colOffsets_[colIdx]);
        colOffsets_[colIdx] += 4;
      }
      long offset = colOffsets_[colIdx] - byteArrayOffset;
      byteArrayVals_[colIdx].set(colData_[colIdx], (int)offset, len);
      colOffsets_[colIdx] += len;
      return byteArrayVals_[colIdx];
    }

    public final TimestampNanos nextTimestampNanos(int colIdx) {
      TimestampNanos timestamp = timestampNanos_[colIdx];
      long millis = unsafe.getLong(colData_[colIdx].array(), colOffsets_[colIdx]);
      colOffsets_[colIdx] += 8;
      int nanos = unsafe.getInt(colData_[colIdx].array(), colOffsets_[colIdx]);
      colOffsets_[colIdx] += 4;
      timestamp.set(millis, nanos);
      return timestamp;
    }

    public final Decimal nextDecimal(int colIdx) {
      int len = byteArrayLen_[colIdx];
      long offset = colOffsets_[colIdx] - byteArrayOffset;
      decimalVals_[colIdx].set(colData_[colIdx], (int)offset, len);
      colOffsets_[colIdx] += len;
      return decimalVals_[colIdx];
    }

    protected Record(Schema schema) {
      recordIdx_ = -1;
      colOffsets_ = new long[schema.cols.size()];
      colData_ = new ByteBuffer[schema.cols.size()];
      byteArrayVals_ = new ByteArray[schema.cols.size()];
      timestampNanos_ = new TimestampNanos[schema.cols.size()];
      decimalVals_ = new Decimal[schema.cols.size()];
      nulls_ = new ByteBuffer[schema.cols.size()];
      byteArrayLen_ = new int[schema.cols.size()];
      schema_ = schema;
      for (int i = 0; i < colOffsets_.length; ++i) {
        Schema.TypeDesc type = schema_.cols.get(i).type;
        if (type.typeId == Schema.Type.STRING || type.typeId == Schema.Type.VARCHAR) {
          byteArrayVals_[i] = new ByteArray();
        }
        if (type.typeId == Schema.Type.TIMESTAMP_NANOS) {
          timestampNanos_[i] = new TimestampNanos();
        }

        if (type.typeId == Schema.Type.CHAR) {
          byteArrayVals_[i] = new ByteArray();
          byteArrayLen_[i] = type.len;
        } else {
          byteArrayLen_[i] = -1;
        }

        if (type.typeId == Schema.Type.DECIMAL) {
          decimalVals_[i] = new Decimal(type.precision, type.scale);
          byteArrayLen_[i] = Decimal.computeByteSize(type.precision, type.scale);
        }
      }
    }

    // Resets the state of the row to return the next batch.
    protected void reset(FetchResult result) throws RuntimeException {
      recordIdx_ = -1;
      numRecords_ = result.numRecords;
      for (int i = 0; i < colOffsets_.length; ++i) {
        nulls_[i] = result.records.cols.get(i).is_null;
        colOffsets_[i] = byteArrayOffset;
        Schema.TypeDesc type = schema_.cols.get(i).type;
        switch (type.typeId) {
          case BOOLEAN:
          case TINYINT:
          case SMALLINT:
          case INT:
          case BIGINT:
          case FLOAT:
          case DOUBLE:
          case STRING:
          case VARCHAR:
          case CHAR:
          case TIMESTAMP_NANOS:
          case DECIMAL:
            colData_[i] = result.records.cols.get(i).data;
            break;
          default:
            throw new RuntimeException(
                "Service returned type that is not supported. Type = " + type);
        }
      }
    }

    public Schema getSchema() { return schema_; }
  }

  // Client and task handle
  private final RecordServiceWorkerClient worker_;
  private RecordServiceWorkerClient.TaskState handle_;

  // Whether to close the worker after we've closed the task
  private boolean closeWorker_;

  // The current fetchResult from the RecordServiceWorker. Never null.
  private FetchResult fetchResult_;

  // Record object that is returned. Reused across calls to next()
  private final Record record_;

  // Cache of result from hasNext(). This starts out as true and makes one
  // transition to false.
  private boolean hasNext_;

  // Completion percentage.
  private float progress_;

  /**
   * Returns true if there are more records.
   */
  public boolean hasNext() throws IOException, RecordServiceException {
    Preconditions.checkNotNull(fetchResult_);
    while (record_.recordIdx_ == record_.numRecords_ - 1) {
      if (fetchResult_.done) {
        hasNext_ = false;
        return false;
      }
      nextBatch();
    }
    return true;
  }

  /**
   * Returns and advances to the next record. The same Record object is
   * returned. Callers need to copy out any values they need to store before
   * calling next() again.
   */
  public Record next() throws IOException {
    if (!hasNext_) throw new IOException("End of stream");
    record_.recordIdx_++;
    return record_;
  }

  /**
   * Closes the task. Idempotent.
   */
  @Override
  public void close() {
    if (handle_ == null) return;
    worker_.closeTask(handle_);
    handle_ = null;
    if (closeWorker_) worker_.close();
  }

  /**
   * Returns the status of the underlying task. This issues an RPC to the server
   * and cannot be used in the hot path.
   */
  public TaskStatus getStatus() throws IOException, RecordServiceException {
    if (handle_ == null) throw new RuntimeException("Task already closed.");
    return worker_.getTaskStatus(handle_);
  }

  /**
   * Returns the schema for the returned records.
   */
  public Schema getSchema() { return record_.getSchema(); }

  /**
   * Set whether to close the worker after the task is done.
   */
  public void setCloseWorker(boolean closeWorker) {
    this.closeWorker_ = closeWorker;
  }

  /**
   * Returns the progress. [0, 1]
   */
  public float progress() { return progress_; }

  protected Records(RecordServiceWorkerClient worker,
      RecordServiceWorkerClient.TaskState handle)
      throws IOException, RecordServiceException {
    worker_ = worker;
    handle_ = handle;
    closeWorker_ = false;

    record_ = new Record(handle.getSchema());
    nextBatch();
    if (record_.schema_.isCountStar) {
      // This is a count(*). We will read the one and only value that is the number of
      // records. The iterator interface will return count(*) number of NULLs.
      Preconditions.checkState(record_.schema_.cols.size() == 1);
      Preconditions.checkState(record_.schema_.cols.get(0).type.typeId == Type.BIGINT);
      Preconditions.checkState(record_.numRecords_ == 1);
      record_.numRecords_ = record_.nextLong(0);
    }
    hasNext_ = hasNext();
  }

  private void nextBatch() throws IOException, RecordServiceException {
    if (handle_ == null) {
      throw new RuntimeException("Task has been closed already.");
    }
    fetchResult_ = worker_.fetch(handle_);
    if (fetchResult_.recordFormat != FetchResult.RecordFormat.Columnar) {
      throw new RuntimeException("Unsupported record format");
    }
    record_.reset(fetchResult_);
    progress_ = (float)fetchResult_.taskProgress;
  }

}
