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

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * Wrapper around ByteBuffer to get a subarray. The purpose of this class is
 * to minimize copies.
 */
// TODO: rethink how to do this optimally. Chances are, we can't get good interop
// with standard objects without a single copy but we want to defer this as late
// as possible to make sure we're not double copying.
// We probably do okay if this returns a byte[] but we'll need to copy into that.
//
// TODO: investigate why ByteBuffer.wrap().slice() doesn't do this.
public class ByteArray {
  private ByteBuffer buffer_;
  private int offset_;
  private int len_;

  /**
   * Creates a ByteArray wrapper object.
   * @param buffer Underlying buffer.
   * @param offset Offset to start in buffer.
   * @param len Length of data.
   */
  public ByteArray(ByteBuffer buffer, int offset, int len) {
    set(buffer, offset, len);
  }

  /**
   * @return offset from byteBuffer() where data starts.
   */
  public int offset() { return offset_; }

  /**
   * @return length of data from byteBuffer().
   */
  public int len() { return len_; }

  /**
   * @return The underlying ByteBuffer.
   */
  public ByteBuffer byteBuffer() { return buffer_; }

  /**
   * Returns the ByteArray as a java String using ASCII encoding. This should
   * not be called in the hot path if possible.
   */
  @Override
  public String toString() {
    return new String(buffer_.array(), offset_, len_, Charset.defaultCharset());
  }

  protected ByteArray() { }

  protected void set(ByteBuffer buffer, int offset, int len) {
    buffer_ = buffer;
    offset_ = offset;
    len_ = len;
  }
}
