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
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.recordservice.thrift.RecordServiceWorker;
import com.cloudera.recordservice.thrift.TErrorCode;
import com.cloudera.recordservice.thrift.TExecTaskParams;
import com.cloudera.recordservice.thrift.TExecTaskResult;
import com.cloudera.recordservice.thrift.TFetchParams;
import com.cloudera.recordservice.thrift.TFetchResult;
import com.cloudera.recordservice.thrift.TRecordServiceException;
import com.cloudera.recordservice.thrift.TUniqueId;
import com.cloudera.recordservice.util.Preconditions;

/**
 * Java client for the RecordServiceWorker. This class is not thread safe.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class RecordServiceWorkerClient implements Closeable {
  private final static Logger LOG =
    LoggerFactory.getLogger(RecordServiceWorkerClient.class);

  // Worker client connection. null if not connected/closed
  private RecordServiceWorker.Client workerClient_;
  private TProtocol protocol_;
  private ProtocolVersion protocolVersion_ = null;
  private String kerberosPrincipal_ = null;
  private DelegationToken delegationToken_ = null;

  // The set of all active tasks.
  private Map<TUniqueId, TaskState> activeTasks_ = new HashMap<TUniqueId, TaskState>();

  // Fetch size to pass to execTask(). If null, server will determine fetch size.
  private Integer fetchSize_ = null;

  // Memory limit to pass to execTask(). If null, server will manage this.
  private Long memLimit_ = null;

  // Maximum number of records to fetch per task.
  private Long limit_ = null;

  // Number of consecutive attempts before failing any request.
  private int maxAttempts_ = 3;

  // Duration to sleep between retry attempts.
  private int retrySleepMs_ = 5000;

  // Millisecond timeout when establishing the connection to the server.
  private int connectionTimeoutMs_ = 10000;

  // Millisecond timeout for TSocket for each RPC to the server, 0 means infinite
  // timeout.
  // This is much longer than connectionTimeoutMs_ typically as the server could be
  // just busy handling the request (e.g. evaluating expensive predicates).
  private int rpcTimeoutMs_ = 120000;

  // Server side logging level. null indicates to use server default.
  private LoggingLevel loggingLevel_ = null;

  /**
   * Per task state maintained in the client.
   */
  public final static class TaskState {
    private TUniqueId handle_;
    private final Task task_;

    // This is the number of rows that have been fetched from the server.
    private int recordsFetched_;
    private final Schema schema_;

    private TaskState(Task task, TExecTaskResult result) {
      task_ = task;
      handle_ = result.handle;
      schema_ = new Schema(result.schema);
    }

    public Schema getSchema() { return schema_; }
  }

  /**
   * Builder to create worker client with various configs.
   * TODO: this has a tone of duplication with RecordServicePlannerClient.Builder().
   * Fix this.
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public final static class Builder {
    RecordServiceWorkerClient client_ = new RecordServiceWorkerClient();

    public Builder() {
      LOG.debug("Creating new worker client connection.");
    }

    public Builder setFetchSize(Integer fetchSize) {
      client_.fetchSize_ = fetchSize;
      LOG.debug("Setting fetch size to " + (fetchSize == null ? "default" : fetchSize));
      return this;
    }

    public Builder setMemLimit(Long memLimit) {
      client_.memLimit_ = memLimit;
      LOG.debug("Setting mem limit to " + (memLimit == null ? "unlimited" : memLimit));
      return this;
    }

    public Builder setLimit(Long limit) {
      client_.limit_ = limit;
      if (limit != null && limit <= 0) {
        throw new IllegalArgumentException(
            "Limit must be greater than 0. Limit=" + limit);
      }
      LOG.debug("Setting limit to " + (limit == null ? "unlimited" : limit));
      return this;
    }

    public Builder setMaxAttempts(int maxAttempts) {
      if (maxAttempts <= 0) {
        throw new IllegalArgumentException("Attempts must be greater than zero.");
      }
      LOG.debug("Setting maxAttempts to " + maxAttempts);
      client_.maxAttempts_ = maxAttempts;
      return this;
    }

    public Builder setSleepDurationMs(int retrySleepMs) {
      if (retrySleepMs < 0) {
        throw new IllegalArgumentException("Sleep duration must be non-negative.");
      }
      LOG.debug("Setting sleep duration to " + retrySleepMs);
      client_.retrySleepMs_ = retrySleepMs;
      return this;
    }

    public Builder setKerberosPrincipal(String principal) {
      if (client_.delegationToken_ != null) {
        // TODO: is this the behavior we want? Maybe try one then the other?
        throw new IllegalStateException(
            "Cannot set both kerberos principal and delegation token.");
      }
      client_.kerberosPrincipal_ = principal;
      return this;
    }

    public Builder setDelegationToken(DelegationToken token) {
      if (client_.kerberosPrincipal_ != null) {
        // TODO: is this the behavior we want? Maybe try one then the other?
        throw new IllegalStateException(
            "Cannot set both kerberos principal and delegation token.");
      }
      client_.delegationToken_ = token;
      return this;
    }

    public Builder setConnectionTimeoutMs(int timeoutMs) {
      if (timeoutMs < 0) {
        throw new IllegalArgumentException(
            "Timeout must not be less than 0. Timeout=" + timeoutMs);
      }
      LOG.debug("Setting connection timeout to " + timeoutMs + "ms");
      client_.connectionTimeoutMs_ = timeoutMs;
      return this;
    }


    public Builder setRpcTimeoutMs(int timeoutMs) {
      if (timeoutMs < 0) {
        throw new IllegalArgumentException(
            "Timeout must not be less than 0. Timeout=" + timeoutMs);
      }
      LOG.debug("Setting RPC timeout to " + timeoutMs + "ms");
      client_.rpcTimeoutMs_ = timeoutMs;
      return this;
    }

    /**
     * Sets the logging level to the level in logger.
     */
    public Builder setLoggingLevel(Logger logger) {
      if (logger == null) {
        client_.loggingLevel_ = null;
      } else if (logger.isTraceEnabled()) {
        client_.loggingLevel_ = LoggingLevel.ALL;
      } else if (logger.isInfoEnabled()) {
        client_.loggingLevel_ = LoggingLevel.INFO;
      } else if (logger.isDebugEnabled()) {
        client_.loggingLevel_ = LoggingLevel.DEBUG;
      } else if (logger.isWarnEnabled()) {
        client_.loggingLevel_ = LoggingLevel.WARN;
      } else if (logger.isErrorEnabled()) {
        client_.loggingLevel_ = LoggingLevel.ERROR;
      } else {
        client_.loggingLevel_ = null;
      }
      LOG.debug("Setting logging level to: " + client_.loggingLevel_);
      return this;
    }

    public Builder setLoggingLevel(LoggingLevel level) {
      client_.loggingLevel_ = level;
      LOG.debug("Setting logging level to: " + client_.loggingLevel_);
      return this;
    }

    /**
     * Creates a worker client connecting to 'hostname'/'port' with previously
     * set options, and the caller must call close().
     */
    public RecordServiceWorkerClient connect(String hostname, int port)
        throws RecordServiceException, IOException {
      client_.connect(hostname, port);
      return client_;
    }

    /**
     * Creates a worker client connecting to the 'address' with previously
     * set options, and the caller must call close().
     */
    public RecordServiceWorkerClient connect(NetworkAddress address)
        throws RecordServiceException, IOException {
      return connect(address.hostname, address.port);
    }
  }

  /**
   * Close the connection to the RecordServiceWorker. All open tasks will also be
   * closed.
   */
  @Override
  public void close() {
    if (workerClient_ != null) {
      LOG.info("Closing RecordServiceWorker connection.");
      for (TUniqueId handle: activeTasks_.keySet()) {
        try {
          workerClient_.CloseTask(handle);
        } catch (TException e) {
          LOG.warn(
              "Failed to close task handle=" + handle + " reason=" + e.getMessage());
        }
      }
      activeTasks_.clear();
      protocol_.getTransport().close();
      workerClient_ = null;
    }
  }

  /**
   * Returns the protocol version of the connected service.
   */
  public ProtocolVersion getProtocolVersion() throws RuntimeException {
    validateIsConnected();
    return protocolVersion_;
  }

  /**
   * Closes the underlying transport, used to simulate an error with the service
   * connection.
   */
  public void closeTask(TaskState handle) {
    validateIsConnected();
    if (activeTasks_.containsKey(handle.handle_)) {
      LOG.info("Closing RecordServiceWorker task: " + handle.handle_);
      try {
        workerClient_.CloseTask(handle.handle_);
      } catch (TException e) {
        LOG.warn(
            "Failed to close task handle=" + handle.handle_ +
            " reason=" + e.getMessage());
      }
      activeTasks_.remove(handle.handle_);
    }
  }

  /**
   * Executes the task asynchronously, returning the handle to the client.
   */
  public TaskState execTask(Task task)
      throws RecordServiceException, IOException {
    validateIsConnected();
    return execTaskInternal(task, 0);
  }

  /**
   * Executes the task asynchronously, returning a Rows object that can be
   * used to fetch results.
   */
  public Records execAndFetch(Task task)
      throws RecordServiceException, IOException {
    validateIsConnected();
    TaskState result = execTaskInternal(task, 0);
    Records records = null;
    try {
      records = new Records(this, result);
      return records;
    } finally {
      if (records == null) closeTask(result);
    }
  }

  /**
   * Fetches a batch of records and returns the result.
   */
  public FetchResult fetch(TaskState state)
      throws RecordServiceException, IOException {
    validateIsConnected();
    validateHandleIsActive(state);
    TException firstException = null;

    boolean connected = true;
    for (int i = 0; i < maxAttempts_; ++i) {
      try {
        if (!connected) {
          connected = waitAndReconnect();
          if (!connected) continue;
        }

        if (LOG.isTraceEnabled()) LOG.trace("Calling fetch(): " + state.handle_);
        TFetchParams fetchParams = new TFetchParams(state.handle_);
        TFetchResult result = workerClient_.Fetch(fetchParams);
        state.recordsFetched_ += result.num_records;
        if (LOG.isTraceEnabled()) {
          LOG.trace("Fetch returned " + result.num_records + " records.");
        }
        return new FetchResult(result);
      } catch (TRecordServiceException e) {
        if (state.task_.resultsOrdered && e.code == TErrorCode.INVALID_HANDLE) {
          LOG.debug("Continuing fault tolerant scan. Record offset=" + state.recordsFetched_);
          // This task returned ordered scans, meaning we can try again and continue
          // the scan. If it is not ordered, we have to fail this task entirely and
          // the client needs to retry it somewhere else.
          activeTasks_.remove(state.handle_);
          TaskState newState = execTaskInternal(state.task_, state.recordsFetched_);
          state.handle_ = newState.handle_;
          continue;
        }
        // There is an error with this request, no point in retrying the request.
        throw new RecordServiceException(e);
      } catch (TException e) {
        // In this case, we've hit a general thrift exception, makes sense to try again.
        connected = false;
        LOG.warn("Failed to fetch(): " + state.handle_ + " ", e);
        if (firstException == null) firstException = e;
      }
    }
    handleThriftException(firstException,
        "Retry attempts exhausted. Could not call fetch.");
    throw new RuntimeException(firstException);
  }

  /**
   * Gets status on the current task executing.
   */
  public TaskStatus getTaskStatus(TaskState handle)
      throws RecordServiceException, IOException {
    validateIsConnected();
    validateHandleIsActive(handle);
    LOG.debug("Calling getTaskStatus(): " + handle.handle_);
    TException firstException = null;
    boolean connected = true;
    for (int i = 0; i < maxAttempts_; ++i) {
      try {
        if (!connected) {
          connected = waitAndReconnect();
          if (!connected) continue;
        }
        return new TaskStatus(workerClient_.GetTaskStatus(handle.handle_));
      } catch (TException e) {
        if (firstException == null) firstException = e;
        connected = false;
      }
    }
    handleThriftException(firstException, "Could not call getTaskStatus.");
    throw new RuntimeException("Could not get task status.");
  }

  /**
   * Returns the number of active tasks for this worker.
   */
  public int numActiveTasks() { return activeTasks_.size(); }

  /**
   * Closes the underlying transport, simulating if the service connection is
   * dropped.
   * As currently implemented, this actually closes the session on the server
   * side meaning all the state on the server is torn down. This does more than
   * simulating network issues, it simulates a server restart.
   * @VisibleForTesting
   */
  void closeConnectionForTesting() {
    protocol_.getTransport().close();
    Preconditions.checkState(!protocol_.getTransport().isOpen());
  }

  // Private ctor, use builder.
  private RecordServiceWorkerClient() { }

  /**
   * Connects to the RecordServiceWorker running on hostname/port.
   * Will retry maxAttempts_ if it got SERVICE_BUSY error.
   */
  private void connect(String hostname, int port)
      throws IOException, RecordServiceException {
    if (workerClient_ != null) {
      throw new RuntimeException("Already connected. Must call close() first.");
    }

    for (int i = 0; i < maxAttempts_; ++i) {
      if (i > 0) {
        LOG.info("Connect to RecordServiceWorker at {}:{} with attempt {}/{}",
            hostname, port, i + 1,  maxAttempts_);
      }
      TTransport transport = ThriftUtils.createTransport("RecordServiceWorker",
          hostname, port, kerberosPrincipal_, delegationToken_, connectionTimeoutMs_);
      protocol_ = new TBinaryProtocol(transport);
      workerClient_ = new RecordServiceWorker.Client(protocol_);
      try {
        // Now that we've connected, set a larger timeout as RPCs that do work can take
        // much longer.
        ThriftUtils.getSocketTransport(transport).setTimeout(rpcTimeoutMs_);
        protocolVersion_ = ThriftUtils.fromThrift(workerClient_.GetProtocolVersion());
        LOG.debug("Connected to RecordServiceWorker with version: " + protocolVersion_);
        if (!protocolVersion_.isValidProtocolVersion()) {
          String errorMsg =
              "Current RecordServiceClient does not support server protocol version: " +
              protocolVersion_.getVersion();
          LOG.warn(errorMsg);
          throw new RecordServiceException(errorMsg, new TRecordServiceException());
        }
        return;
      } catch (TRecordServiceException e) {
        // For 'GetProtocolVersion' call, the server side will first establish
        // the connection, and then throws the exception when it's processing the call.
        // The client is responsible to close the connection after seeing the exception.
        close();
        if (i + 1 <  maxAttempts_ && e.getCode() == TErrorCode.SERVICE_BUSY) {
          // Only retry when service is busy.
          LOG.warn("Failed to connect: ", e);
          sleepForRetry();
          continue;
        }
        LOG.warn("Connection is rejected because RecordServiceWorker has reached the " +
            "maximum number of connections it is able to handle.");
        throw new RecordServiceException("Connection to RecordServiceWorker at " +
            hostname + ":" + port + " is rejected. ", e);
      } catch (TTransportException e) {
        close();
        if (e.getType() == TTransportException.END_OF_FILE) {
          TRecordServiceException ex = new TRecordServiceException();
          ex.code = TErrorCode.AUTHENTICATION_ERROR;
          ex.message = "Connection to RecordServiceWorker at " + hostname + ":" + port +
              " has failed. Please check if the client has the same security setting " +
              "as the server.";
          LOG.warn(ex.message, ex);
          throw new RecordServiceException(ex);
        }
        String errorMsg = "Could not get service protocol version from " +
            "RecordServiceWorker at " + hostname + ":" + port + ". ";
        if ((e.getCause() instanceof SocketTimeoutException) &&
            e.getCause().getMessage().contains("Read timed out")) {
          errorMsg += " Got SocketTimeoutException: Read timed out, you may increase " +
              "recordservice.worker.rpc.timeoutMs.";
        }
        LOG.warn(errorMsg, e);
        throw new IOException(errorMsg, e);
      } catch (TException e) {
        close();
        String errorMsg = "Could not get service protocol version. It's likely " +
            "the service at " + hostname + ":" + port + " is not running the " +
            "RecordServiceWorker. ";
        LOG.warn(errorMsg, e);
        throw new IOException(errorMsg, e);
      }
    }
  }

  /**
   * Executes the task asynchronously, returning the handle the client.
   */
  private TaskState execTaskInternal(Task task, long offset)
          throws RecordServiceException, IOException {
    Preconditions.checkNotNull(task);
    Preconditions.checkState(offset >= 0);
    TExecTaskParams taskParams = new TExecTaskParams(ByteBuffer.wrap(task.task));
    // Only wrap tag in TExecTaskParams when it is not an empty string.
    if (!task.getTag().isEmpty()) {
      taskParams.setTag(task.getTag());
    }
    taskParams.setOffset(offset);
    if (fetchSize_ != null) taskParams.setFetch_size(fetchSize_);
    if (memLimit_ != null) taskParams.setMem_limit(memLimit_);
    if (limit_ != null) taskParams.setLimit(limit_);
    if (loggingLevel_ != null) taskParams.setLogging_level(loggingLevel_.toThrift());

    TException firstException = null;
    boolean connected = true;
    for (int i = 0; i < maxAttempts_; ++i) {
      try {
        if (!connected) {
          connected = waitAndReconnect();
          if (!connected) continue;
        }
        LOG.debug("Executing task attempt " + (i + 1) + " out of " + maxAttempts_ +
            ". Offset=" + offset);
        TExecTaskResult result = workerClient_.ExecTask(taskParams);
        Preconditions.checkState(!activeTasks_.containsKey(result.handle));
        TaskState state = new TaskState(task, result);
        activeTasks_.put(result.handle, state);
        LOG.info("Got task handle: " + result.handle);
        return state;
      } catch (TRecordServiceException e) {
        switch (e.code) {
          case SERVICE_BUSY:
            sleepForRetry();
            continue;
          default:
            throw new RecordServiceException(e);
        }
      } catch (TException e) {
        if (firstException == null) firstException = e;
        connected = false;
      }
    }
    handleThriftException(firstException, "Could not exec task.");
    throw new RuntimeException("Could not exec task.");
  }

  /**
   * Sleeps for retrySleepMs_ and reconnects to the worker. Returns
   * true if the connection was established.
   * TODO: why does this behave so differently than the Planner implementation.
   */
  private boolean waitAndReconnect() {
    sleepForRetry();
    try {
      protocol_.getTransport().open();
      TSocket socket = ThriftUtils.getSocketTransport(protocol_.getTransport());
      socket.setTimeout(connectionTimeoutMs_);
      workerClient_ = new RecordServiceWorker.Client(protocol_);
      socket.setTimeout(rpcTimeoutMs_);
      return true;
    } catch (TTransportException e) {
      return false;
    }
  }

  /**
   * Sleeps for retrySleepMs_.
   */
  private void sleepForRetry() {
    if (LOG.isInfoEnabled()) {
      LOG.info("Sleeping for " + retrySleepMs_ + "ms before retrying.");
    }
    try {
      Thread.sleep(retrySleepMs_);
    } catch (InterruptedException e) {
      LOG.error("Failed sleeping: " + e);
    }
  }

  /**
   * Handles TException, throwing a more canonical exception.
   * generalMsg is thrown if we can't infer more information from e.
   */
  private void handleThriftException(TException e, String generalMsg)
      throws RecordServiceException, IOException {
    // TODO: this should mark the connection as bad on some error codes.
    if (e instanceof TRecordServiceException) {
      throw new RecordServiceException((TRecordServiceException)e);
    } else if (e instanceof TTransportException) {
      LOG.warn("Could not reach worker service.");
      throw new IOException("Could not reach service.", e);
    } else {
      throw new IOException(generalMsg, e);
    }
  }

  private void validateIsConnected() throws RuntimeException {
    if (workerClient_ == null) throw new RuntimeException("Client not connected.");
  }

  private void validateHandleIsActive(TaskState state) {
    if (!activeTasks_.containsKey(state.handle_)) {
      throw new IllegalArgumentException("Invalid task handle.");
    }
  }
}
