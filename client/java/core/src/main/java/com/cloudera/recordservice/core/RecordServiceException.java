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

import com.cloudera.recordservice.thrift.TErrorCode;
import com.cloudera.recordservice.thrift.TRecordServiceException;

/**
 * POJO for TRecordServiceException
 */
public class RecordServiceException extends Exception {
  private static final long serialVersionUID = 6081200485685697949L;

  public static enum ErrorCode {
    UNKNOWN,
    AUTHENTICATION_ERROR,
    CANCELLED,
    CONNECTION_TIMED_OUT,
    INTERNAL_ERROR,
    INVALID_HANDLE,
    INVALID_REQUEST,
    INVALID_TASK,
    OUT_OF_MEMORY,
    SERVICE_BUSY;

    static ErrorCode fromThrift(TErrorCode c) {
      switch (c) {
        case AUTHENTICATION_ERROR: return AUTHENTICATION_ERROR;
        case CANCELLED: return CANCELLED;
        case CONNECTION_TIMED_OUT: return CONNECTION_TIMED_OUT;
        case INTERNAL_ERROR: return INTERNAL_ERROR;
        case INVALID_HANDLE: return INVALID_HANDLE;
        case INVALID_REQUEST: return INVALID_REQUEST;
        case INVALID_TASK: return INVALID_TASK;
        case OUT_OF_MEMORY: return OUT_OF_MEMORY;
        case SERVICE_BUSY: return SERVICE_BUSY;
        default:
          return UNKNOWN;
      }
    }
  }

  public final ErrorCode code;
  public final String message;
  public final String detail;

  RecordServiceException(TRecordServiceException e) {
    super(e);
    code = ErrorCode.fromThrift(e.code);
    message = e.message;
    detail = e.detail;
  }

  RecordServiceException(String msg, TRecordServiceException e) {
    super(msg, e);
    code = ErrorCode.fromThrift(e.code);
    message = msg + e.message;
    detail = e.detail;
  }
}
