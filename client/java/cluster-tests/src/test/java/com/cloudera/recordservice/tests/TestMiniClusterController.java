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

package com.cloudera.recordservice.tests;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.cloudera.recordservice.examples.mapreduce.WordCount;
import com.cloudera.recordservice.examples.mapreduce.WordCount.Map;
import com.cloudera.recordservice.examples.mapreduce.WordCount.Reduce;

public class TestMiniClusterController {
  public static final int DEFAULT_NODE_NUM = 3;
  static Random rand_ = new Random();
  MiniClusterController miniCluster_;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    org.apache.log4j.BasicConfigurator.configure();
    MiniClusterController.Start(DEFAULT_NODE_NUM);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }

  @Before
  public void setUp() throws Exception {
    miniCluster_ = MiniClusterController.instance();
    assertTrue("Cluster is in incorrect state!", miniCluster_.isClusterStateCorrect());
  }

  @After
  public void tearDown() throws Exception {
  }

  public static JobConf createWordCountMRJobConf()  {
    JobConf conf = new JobConf(WordCount.class);
    fillInWordCountMRJobConf(conf);
    return conf;
  }

  public static void setRandomOutputDir(JobConf conf) {
    Integer intSuffix = rand_.nextInt(10000000);
    String suffix = intSuffix.toString();
    String outDir = "/tmp/" + conf.getJobName() + "_" + suffix;
    System.out.println("outdir: " + outDir);
    FileOutputFormat.setOutputPath(conf, new Path(outDir));
  }

  // TODO: Move this and the following test case to a test library
  public static void fillInWordCountMRJobConf(JobConf conf) {
    String input = "select n_comment from tpch.nation";

    conf.setJobName("samplejob-wordcount");

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);

    conf.setMapperClass(Map.class);
    conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);

    conf.setInputFormat(com.cloudera.recordservice.mapred.TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);
    com.cloudera.recordservice.mr.RecordServiceConfig.setInputQuery(conf, input);
    setRandomOutputDir(conf);
  }

  /**
   * This method creates a sample MR job and submits that JobConf object to the
   * static MiniClusterController method to be executed.
   */
  @Test
  public void testRunningJobLocally() throws IOException, InterruptedException {
    JobConf sampleJob = createWordCountMRJobConf();
    RunningJob runningJob = miniCluster_.runJob(sampleJob);
    runningJob.waitForCompletion();
    assertTrue(runningJob.isSuccessful());
  }

  /**
   * This method gets a JobConf object from the static MiniClusterController
   * method, fills it with a sample MR job and then executes the job.
   */
  @Test
  public void testGetConfigForMiniCluster() throws IOException {
    JobConf sampleJob = miniCluster_.getJobConf(WordCount.class);
    fillInWordCountMRJobConf(sampleJob);
    RunningJob runningJob = JobClient.runJob(sampleJob);
    runningJob.waitForCompletion();
    assertTrue(runningJob.isSuccessful());
  }

  /**
   * This method adds a node to the cluster and then checks to make sure that
   * the cluster state is correct.
   */
  @Test
  public void testClusterHealth() throws IOException {
    assertTrue("Cluster is in incorrect state!", miniCluster_.isClusterStateCorrect());
    miniCluster_.addRecordServiced();
    assertTrue("Cluster is in incorrect state!", miniCluster_.isClusterStateCorrect());
  }

  /**
   * This method adds a node to the cluster and checks that the cluster state is
   * still correct.
   */
  @Test
  public void testAddNode() throws InterruptedException {
    assertTrue("Cluster size is incorrect!",
        miniCluster_.getClusterSize() == DEFAULT_NODE_NUM);
    miniCluster_.addRecordServiced();
    assertTrue("Cluster size is incorrect!",
        miniCluster_.getClusterSize() == (DEFAULT_NODE_NUM + 1));
    assertTrue("Cluster is in incorrect state!", miniCluster_.isClusterStateCorrect());
  }
}
