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

import com.mongodb.internal.connection.tlschannel.util.DirectBufferDeallocator;

import java.nio.ByteBuffer;

/**
 * Allocator that creates direct buffers. The {@link #free(ByteBuffer)} method, if called,
 * deallocates the buffer immediately, without having to wait for GC (and the finalizer) to run.
 * Calling {@link #free(ByteBuffer)} is actually optional, but should result in reduced memory
 * consumption.
 *
 * <p>Direct buffers are generally preferred for using with I/O, to avoid an extra user-space copy,
 * or to reduce garbage collection overhead.
 */
public class DirectBufferAllocator implements BufferAllocator {

  private final DirectBufferDeallocator deallocator = new DirectBufferDeallocator();

  @Override
  public ByteBuffer allocate(int size) {
    return ByteBuffer.allocateDirect(size);
  }

  @Override
  public void free(ByteBuffer buffer) {
    // do not wait for GC (and finalizer) to run
    deallocator.deallocate(buffer);
  }
}
