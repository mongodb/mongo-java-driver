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

package com.mongodb.internal.connection.tlschannel;

import java.nio.ByteBuffer;

/**
 * Allocator that creates heap buffers. The {@link #free(ByteBuffer)} method is a no-op, as heap
 * buffers are handled completely by the garbage collector.
 *
 * <p>Direct buffers are generally used as a simple and generally good enough default solution.
 */
public class HeapBufferAllocator implements BufferAllocator {

  @Override
  public ByteBuffer allocate(int size) {
    return ByteBuffer.allocate(size);
  }

  @Override
  public void free(ByteBuffer buffer) {
    // GC does it
  }
}
