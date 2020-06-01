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

import java.nio.Buffer;
import java.nio.ByteBuffer;

public class ByteBufferUtil {

  public static void copy(ByteBuffer src, ByteBuffer dst, int length) {
    if (length < 0) {
      throw new IllegalArgumentException("negative length");
    }
    if (src.remaining() < length) {
      throw new IllegalArgumentException(
          String.format(
              "source buffer does not have enough remaining capacity (%d < %d)",
              src.remaining(), length));
    }
    if (dst.remaining() < length) {
      throw new IllegalArgumentException(
          String.format(
              "destination buffer does not have enough remaining capacity (%d < %d)",
              dst.remaining(), length));
    }
    if (length == 0) {
      return;
    }
    ByteBuffer tmp = src.duplicate();
    ((Buffer) tmp).limit(src.position() + length);
    dst.put(tmp);
    ((Buffer) src).position(src.position() + length);
  }
}
