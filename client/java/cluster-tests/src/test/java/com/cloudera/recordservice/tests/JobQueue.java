// Copyright 2015 Cloudera Inc.
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

package com.cloudera.recordservice.tests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class contains a queue of jobs that can be executed on the
 * RecordService. There are also methods that check job completion.
 */
public class JobQueue {
  public static final int DEFAULT_NUM_NODES = 3;
  public static final int DEFAULT_NUM_THREADS = 5;
  public static final int DEFAULT_PER_JOB_TIMEOUT_VALUE_IN_SECONDS = 20;

  private static final Logger LOGGER = LoggerFactory.getLogger(JobQueue.class);

  // The queue's thread pool
  private ThreadPoolExecutor threadPoolExecutor_;
  // Jobs started by the threadPoolExecutor
  private List<Future> synchedJobList_;
  private ClusterController cluster_;
  // If true, all completed jobs have been successful. Used by completion daemon
  private boolean successful_ = true;
  // Length of timeout in seconds
  private int timeoutSecs_ = 0;

  /**
   * Creation of a JobQueue object get an instance of a running mini cluster,
   * which will start the mini cluster if it is not currently running.
   */
  public JobQueue(int workerThreads) {
    synchedJobList_ = Collections.synchronizedList(new ArrayList<Future>());
    // miniCluster_ = MiniClusterController.instance();
    cluster_ = ClusterController.cluster_;
    startThreadPool();
  }

  public boolean killQueue() {
    List<Runnable> l = threadPoolExecutor_.shutdownNow();
    System.out.println("After kill call");
    return l != null;
  }

  /**
   * This class is a wrapper for a JobConf that allows it to be executed in a
   * ThreadPoolExecutor
   */
  public final class RSMRJob implements Callable {
    JobConf conf_;
    RunningJob job_;

    public RSMRJob(JobConf conf) {
      conf_ = conf;
    }

    @Override
    public Object call() throws Exception {
      job_ = cluster_.runJob(conf_);
      return job_;
    }
  }

  /**
   * The daemon traverses the active jobs list checking for completed jobs. Jobs
   * that have been marked as successful are removed from the synchedJobList_.
   * If the daemon finds an unsuccessful job it changes the global boolean
   * successful_ variable to false and shuts down the threadPoolExecutor. There
   * should only be a maximum of one JobCompletionDaemons per JobQueue.
   */
  private final class JobCompletionDaemon implements Runnable {
    @Override
    public void run() {
      while (!threadPoolExecutor_.isTerminated()) {
        if (synchedJobList_.isEmpty()) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          continue;
        }
        synchronized (synchedJobList_) {
          Iterator<Future> it = synchedJobList_.iterator();
          while (it.hasNext()) {
            Future f = it.next();
            try {
              RunningJob job = (RunningJob) f.get();
              if (job.isComplete()) {
                if (!job.isSuccessful()) {
                  threadPoolExecutor_.shutdownNow();
                  successful_ = false;
                }
                it.remove();
              }
            } catch (IOException e) {
              LOGGER.debug(e.getStackTrace().toString());
            } catch (ExecutionException ee) {
              LOGGER.debug(ee.getStackTrace().toString());
            } catch (InterruptedException ie) {
              LOGGER.debug(ie.getStackTrace().toString());
            }
          }
        }
        // Currently the process of the daemon checking the active jobs list
        // locks the list. We sleep here to allow new jobs to be added to
        // the list.
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          LOGGER.debug(e.getStackTrace().toString());
        }
      }
    }
  }

  /**
   * Checks the synched job list returning true if all jobs marked as completed
   * were also marked as successful. Successful jobs are removed from the list.
   * On the first found failure, the method returns false and does not remove
   * said job from the list.
   */
  public boolean checkCompleted() {
    List<RunningJob> theFailureList = new LinkedList<RunningJob>();
    synchronized (synchedJobList_) {
      Iterator<Future> it = synchedJobList_.iterator();
      while (it.hasNext()) {
        Future f = it.next();
        try {
          RunningJob job = (RunningJob) f.get();
          if (job.isComplete()) {
            if (!job.isSuccessful()) {
              successful_ = false;
              theFailureList.add(job);
              return successful_;
            }
            it.remove();
          }
        } catch (IOException e) {
          LOGGER.debug(e.getStackTrace().toString());
          e.printStackTrace();
          successful_ = false;
          return successful_;
        } catch (ExecutionException ee) {
          LOGGER.debug(ee.getStackTrace().toString());
          ee.printStackTrace();
          successful_ = false;
          return successful_;
        } catch (InterruptedException ie) {
          ie.printStackTrace();
          LOGGER.debug(ie.getStackTrace().toString());
          successful_ = false;
          return successful_;
        }
      }
    }
    successful_ = true;
    Iterator<RunningJob> it = theFailureList.iterator();
    while (it.hasNext()) {
      System.out.println(it.next().getID());
    }
    return successful_;
  }

  /**
   * This method adds the given collection to the job queue and then puts the
   * resulting Futures into the synchedJobList_. The method also adjusts the
   * timeout accordingly.
   */
  public <T> void addJobsToQueue(Collection<? extends Callable<T>> tasks)
      throws InterruptedException {
    setTimeout((DEFAULT_PER_JOB_TIMEOUT_VALUE_IN_SECONDS * tasks.size()) + timeoutSecs_);
    List<Future<T>> futureList = threadPoolExecutor_.invokeAll(tasks, timeoutSecs_,
        TimeUnit.SECONDS);
    synchedJobList_.addAll(futureList);
  }

  public void setTimeout(int seconds) {
    timeoutSecs_ = seconds;
  }

  public void startThreadPool() {
    threadPoolExecutor_ = new ThreadPoolExecutor(DEFAULT_NUM_THREADS,
        DEFAULT_NUM_THREADS * 2, 0, TimeUnit.SECONDS, new LinkedBlockingDeque<Runnable>());
  }
}
