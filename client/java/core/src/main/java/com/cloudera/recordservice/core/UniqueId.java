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

import java.io.Serializable;

import com.cloudera.recordservice.thrift.TUniqueId;

/**
 * POJO for TUniqueId
 */
public class UniqueId implements Serializable {
  private static final long serialVersionUID = -5643572131083930434L;

  public final long hi;
  public final long lo;

  UniqueId(TUniqueId id) {
    hi = id.hi;
    lo = id.lo;
  }

  public UniqueId(long hi, long lo) {
    this.hi = hi;
    this.lo = lo;
  }

  @Override
  public String toString() {
    return String.format("%x:%x", hi, lo);
  }

  TUniqueId toThrift() { return new TUniqueId(hi, lo); }
}
