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

package com.cloudera.recordservice.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.cloudera.recordservice.core.NetworkAddress;
import com.cloudera.recordservice.core.PlanRequestResult;
import com.cloudera.recordservice.core.RecordServiceException;
import com.cloudera.recordservice.core.RecordServicePlannerClient;
import com.cloudera.recordservice.core.RecordServiceWorkerClient;
import com.cloudera.recordservice.core.Records;
import com.cloudera.recordservice.core.Records.Record;
import com.cloudera.recordservice.core.Request;
import com.cloudera.recordservice.core.Schema;
import com.cloudera.recordservice.core.Schema.ColumnDesc;
import com.cloudera.recordservice.core.Task;
import com.google.common.base.Joiner;

public class RSCat {
  /**
   * RSCat: a program to cat a file/table similar to hadoop -cat
   *
   * Usage: RSCat file/tablename [number of lines/rows] [--hostname host]
   * [--port port]
   *
   * RSCat works on any file accessible by RecordService. If no number of
   * lines/rows value is provided then RSCat return the whole file.
   */
  static final String USAGE = "Usage: RSCat file/table [number of rows] "
      + "[--hostname host] [--port port]";

  public static List<Object> processRow(Record r, Schema schema) {
    List<Object> returnList = new ArrayList<Object>();
    for (int i = 0; i < schema.cols.size(); ++i) {
      ColumnDesc col = schema.cols.get(i);
      switch (col.type.typeId) {
      case BOOLEAN:
        returnList.add(new Boolean(r.nextBoolean(i)));
        break;
      case TINYINT:
        returnList.add(new Byte(r.nextByte(i)));
        break;
      case SMALLINT:
        returnList.add(r.nextShort(i));
        break;
      case INT:
        returnList.add(r.nextInt(i));
        break;
      case BIGINT:
        returnList.add(r.nextLong(i));
        break;
      case FLOAT:
        returnList.add(r.nextFloat(i));
        break;
      case DOUBLE:
        returnList.add(r.nextDouble(i));
        break;
      case STRING:
        returnList.add(r.nextByteArray(i).toString());
        break;
      case VARCHAR:
        returnList.add(r.nextByteArray(i).toString());
        break;
      case CHAR:
        returnList.add(r.nextByte(i));
        break;
      case TIMESTAMP_NANOS:
        returnList.add(r.nextTimestampNanos(i).toTimeStamp());
        break;
      case DECIMAL:
        returnList.add(r.nextDecimal(i));
        break;
      default:
        throw new RuntimeException("Service returned type that is not supported. Type = "
            + col.type.typeId);
      }
    }
    return returnList;
  }

  public static void processPath(String path, int numRecords, String hostname, int port)
      throws RecordServiceException, IOException {
    RecordServicePlannerClient rspc = null;
    Request planRequest;
    PlanRequestResult planResult;
    try {
      rspc = new RecordServicePlannerClient.Builder().connect(hostname, port);
      planRequest = Request.createPathRequest(path);
      planResult = rspc.planRequest(planRequest);
    } catch (RecordServiceException rse) {
      // This try catch is used to detect the request type. If the path request
      // fails, we know that path is either a table scan request or doesn't
      // exist
      planRequest = Request.createTableScanRequest(path);
      planResult = rspc.planRequest(planRequest);
    } finally {
      if (rspc != null) {
        rspc.close();
      }
    }

    Random randGen = new Random();
    RecordServiceWorkerClient rswc = null;
    for (int i = 0; i < planResult.tasks.size(); ++i) {
      Records rds = null;
      try {
        Task task = planResult.tasks.get(i);
        task.setTag("RSCat");
        NetworkAddress addr;
        if (task.localHosts.size() > 0) {
          int hostChoice = randGen.nextInt(task.localHosts.size());
          addr = task.localHosts.get(hostChoice);
        } else {
          int hostChoice = randGen.nextInt(planResult.hosts.size());
          addr = planResult.hosts.get(hostChoice);
        }
        rswc = new RecordServiceWorkerClient.Builder().connect(addr.hostname, addr.port);
        rds = rswc.execAndFetch(task);
        Schema taskSchema = rds.getSchema();
        Record record;
        while (rds.hasNext()) {
          record = rds.next();
          System.out.println(Joiner.on(",").join(processRow(record, taskSchema)));
        }
      } finally {
        if (rds != null){
          rds.close();
        }
        if (rswc != null) {
          rswc.close();
        }
      }
    }
  }

  public static void main(String[] args) {
    if (args.length == 0) {
      System.err.println(USAGE);
      return;
    }

    Integer numRecords = Integer.MAX_VALUE;
    int port = 12050;
    String hostname = "localhost";
    String filename = "";
    try {
      for (int i = 0; i < args.length; ++i) {
        if (args[i].equals("--help") || args[i].equals("-h")) {
          System.err.println(USAGE);
          return;
        }
        if (args[i].equals("--hostname") || args[i].equals("-hostname")) {
          hostname = args[i + 1];
          ++i;
        } else if (args[i].equals("--port") || args[i].equals("-p")
            || args[i].equals("-port")) {
          port = Integer.parseInt(args[i + 1]);
          ++i;
        } else if (filename.equals("")) {
          filename = args[i];
        } else {
          numRecords = Integer.parseInt(args[i]);
        }
      }
    } catch (ArrayIndexOutOfBoundsException e) {
      System.err.println("Arguments not formatted correctly.\n" + USAGE);
      return;
    } catch (NumberFormatException nfe) {
      System.err.println("Arguments not formatted correctly.\n" + USAGE);
      return;
    }

    try {
      processPath(filename, numRecords, hostname, port);
    } catch (RecordServiceException e) {
      System.err.println(e);
      System.exit(1);
    } catch (IOException io) {
      System.err.println(io);
      System.exit(1);
    }
  }
}
