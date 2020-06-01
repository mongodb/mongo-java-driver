/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Original Work: MIT License, Copyright (c) [2015-2020] all contributors
 * https://github.com/marianobarrios/tls-channel
 */

package com.mongodb.internal.connection.tlschannel.impl;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class ByteBufferSet {

  public final ByteBuffer[] array;
  public final int offset;
  public final int length;

  public ByteBufferSet(ByteBuffer[] array, int offset, int length) {
    if (array == null) throw new NullPointerException();
    if (array.length < offset) throw new IndexOutOfBoundsException();
    if (array.length < offset + length) throw new IndexOutOfBoundsException();
    for (int i = offset; i < offset + length; i++) {
      if (array[i] == null) throw new NullPointerException();
    }
    this.array = array;
    this.offset = offset;
    this.length = length;
  }

  public ByteBufferSet(ByteBuffer[] array) {
    this(array, 0, array.length);
  }

  public ByteBufferSet(ByteBuffer buffer) {
    this(new ByteBuffer[] {buffer});
  }

  public long remaining() {
    long ret = 0;
    for (int i = offset; i < offset + length; i++) {
      ret += array[i].remaining();
    }
    return ret;
  }

  public int putRemaining(ByteBuffer from) {
    int totalBytes = 0;
    for (int i = offset; i < offset + length; i++) {
      if (!from.hasRemaining()) break;
      ByteBuffer dstBuffer = array[i];
      int bytes = Math.min(from.remaining(), dstBuffer.remaining());
      ByteBufferUtil.copy(from, dstBuffer, bytes);
      totalBytes += bytes;
    }
    return totalBytes;
  }

  public ByteBufferSet put(ByteBuffer from, int length) {
    if (from.remaining() < length) {
      throw new IllegalArgumentException();
    }
    if (remaining() < length) {
      throw new IllegalArgumentException();
    }
    int totalBytes = 0;
    for (int i = offset; i < offset + this.length; i++) {
      int pending = length - totalBytes;
      if (pending == 0) break;
      int bytes = Math.min(pending, (int) remaining());
      ByteBuffer dstBuffer = array[i];
      ByteBufferUtil.copy(from, dstBuffer, bytes);
      totalBytes += bytes;
    }
    return this;
  }

  public int getRemaining(ByteBuffer dst) {
    int totalBytes = 0;
    for (int i = offset; i < offset + length; i++) {
      if (!dst.hasRemaining()) break;
      ByteBuffer srcBuffer = array[i];
      int bytes = Math.min(dst.remaining(), srcBuffer.remaining());
      ByteBufferUtil.copy(srcBuffer, dst, bytes);
      totalBytes += bytes;
    }
    return totalBytes;
  }

  public ByteBufferSet get(ByteBuffer dst, int length) {
    if (remaining() < length) {
      throw new IllegalArgumentException();
    }
    if (dst.remaining() < length) {
      throw new IllegalArgumentException();
    }
    int totalBytes = 0;
    for (int i = offset; i < offset + this.length; i++) {
      int pending = length - totalBytes;
      if (pending == 0) break;
      ByteBuffer srcBuffer = array[i];
      int bytes = Math.min(pending, srcBuffer.remaining());
      ByteBufferUtil.copy(srcBuffer, dst, bytes);
      totalBytes += bytes;
    }
    return this;
  }

  public boolean hasRemaining() {
    return remaining() > 0;
  }

  public boolean isReadOnly() {
    for (int i = offset; i < offset + length; i++) {
      if (array[i].isReadOnly()) return true;
    }
    return false;
  }

  @Override
  public String toString() {
    return "ByteBufferSet[array="
        + Arrays.toString(array)
        + ", offset="
        + offset
        + ", length="
        + length
        + "]";
  }
}
