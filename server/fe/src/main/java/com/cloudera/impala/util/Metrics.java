// Copyright 2012 Cloudera Inc.
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

package com.cloudera.impala.util;

import java.util.concurrent.TimeUnit;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

/**
 * Provide a singleton instance to add some metrics, such as timer and histogram. There
 * are multiple reporters which will generate metrics reports periodically, such as
 * console reporter and JMX reporter. Here is an example of timer report by console
 * reporter:
 *
 *  7/28/15 3:32:56 PM =============================================================
 *
 * -- Timers ----------------------------------------------------------------------
 * LOAD_HDFSTABLE_METADATA.scale_db.num_partitions_100_blocks_per_partition_100
 *              count = 1
 *          mean rate = 0.00 calls/second
 *      1-minute rate = 0.00 calls/second
 *      5-minute rate = 0.00 calls/second
 *     15-minute rate = 0.00 calls/second
 *                min = 2964.22 milliseconds
 *                max = 2964.22 milliseconds
 *               mean = 2964.22 milliseconds
 *             stddev = 0.00 milliseconds
 *             median = 2964.22 milliseconds
 *               75% <= 2964.22 milliseconds
 *               95% <= 2964.22 milliseconds
 *               98% <= 2964.22 milliseconds
 *               99% <= 2964.22 milliseconds
 *             99.9% <= 2964.22 milliseconds
 *
 */
public enum Metrics {
  // Enum singleton ensures only one MetricRegistry is initialized,
  // and all metrics results are generated by the same reporters.
  INSTANCE;

  // Container for all metrics.
  private final MetricRegistry metricRegistry_;
  // Report to JMX MBeans.
  private final JmxReporter jmxReporter_;
  public final static int NANOTOMILLISEC = 1000000;

  Metrics() {
    metricRegistry_ = new MetricRegistry();
    // Start a JMX reporter.
    jmxReporter_ = JmxReporter.forRegistry(metricRegistry_)
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .build();
    jmxReporter_.start();
  }

  /**
   * Return the timer context which will measure the amount of time in nanoseconds.
   * Timer is stated when this method is called, and you can call context.stop()
   * or context.close() to stop Timer.
   */
  public Timer.Context getTimerCtx(String name) {
    return metricRegistry_.timer(name).time();
  }

  /**
   * Return the histogram which will measure the distribution of values in a stream of
   * data.
   */
  public Histogram getHistogram(String name) {
    return metricRegistry_.histogram(name);
  }
}
