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

import java.util.List;

import com.cloudera.recordservice.thrift.TPathRequest;
import com.cloudera.recordservice.thrift.TPlanRequestParams;
import com.cloudera.recordservice.thrift.TRequestType;

/**
 * Abstraction over different requests and utilities to build common request
 * types.
 */
public class Request {
  /**
   * Creates a request that is a SQL query.
   */
  public static Request createSqlRequest(String query) {
    TPlanRequestParams request = new TPlanRequestParams();
    request.request_type = TRequestType.Sql;
    request.sql_stmt = query;
    return new Request(request);
  }

  /**
   * Creates a request to read an entire table.
   */
  public static Request createTableScanRequest(String table) {
    TPlanRequestParams request = new TPlanRequestParams();
    request.request_type = TRequestType.Sql;
    request.sql_stmt = "SELECT * FROM " + table;
    return new Request(request);
  }

  /**
   * Creates a request to read a projection of a table. An empty or null
   * projection returns the number of rows in the table (as a BIGINT).
   */
  public static Request createProjectionRequest(String table, List<String> cols) {
    TPlanRequestParams request = new TPlanRequestParams();
    request.request_type = TRequestType.Sql;
    if (cols == null || cols.size() == 0) {
      request.sql_stmt = "SELECT count(*) FROM " + table;
    } else {
      StringBuilder sb = new StringBuilder("SELECT ");
      for (int i = 0; i < cols.size(); ++i) {
        if (i != 0) sb.append(", ");
        sb.append(cols.get(i));
      }
      sb.append(" FROM ").append(table);
      request.sql_stmt = sb.toString();
    }
    return new Request(request);
  }

  /**
   * Creates a request that is a PATH query. This does a full scan of the
   * data files in 'uri'.
   */
  public static Request createPathRequest(String uri) {
    TPlanRequestParams request = new TPlanRequestParams();
    request.request_type = TRequestType.Path;
    request.path = new TPathRequest(uri);
    return new Request(request);
  }

  /**
   * Sets a query for a path request. Invalid for non-path requests.
   */
  public Request setQuery(String query) {
    verifyPathRequest("setQuery()");
    request_.path.setQuery(query);
    return this;
  }

  /**
   * Sets the schema for a path request. Invalid for non-path requests.
   */
  public Request setSchema(Schema schema) {
    verifyPathRequest("setSchema()");
    request_.path.setSchema(schema.toThrift());
    return this;
  }

  /**
   * Sets the field delimiter for a path request. Invalid for non-path requests.
   */
  public Request setFieldDelimiter(char delimiter) {
    verifyPathRequest("setFieldDelimiter()");
    request_.path.setField_delimiter((byte)delimiter);
    return this;
  }

  @Override
  public String toString() {
    return request_.toString();
  }

  TPlanRequestParams request_;

  private Request(TPlanRequestParams request) {
    request_ = request;
  }

  /**
   * Verifies this request is a path request, throwing an exception if it is not.
   */
  private void verifyPathRequest(String callingFunction) {
    if (request_.request_type != TRequestType.Path) {
      throw new IllegalArgumentException(callingFunction +
          " is only callable for path requests.");
    }
  }
}
