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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;

/** A decorating {@link BufferAllocator} that keeps statistics. */
public class TrackingAllocator implements BufferAllocator {

  private BufferAllocator impl;

  private LongAdder bytesAllocatedAdder = new LongAdder();
  private LongAdder bytesDeallocatedAdder = new LongAdder();
  private AtomicLong currentAllocationSize = new AtomicLong();
  private LongAccumulator maxAllocationSizeAcc = new LongAccumulator(Math::max, 0);

  private LongAdder buffersAllocatedAdder = new LongAdder();
  private LongAdder buffersDeallocatedAdder = new LongAdder();

  public TrackingAllocator(BufferAllocator impl) {
    this.impl = impl;
  }

  public ByteBuffer allocate(int size) {
    bytesAllocatedAdder.add(size);
    currentAllocationSize.addAndGet(size);
    buffersAllocatedAdder.increment();
    return impl.allocate(size);
  }

  public void free(ByteBuffer buffer) {
    int size = buffer.capacity();
    bytesDeallocatedAdder.add(size);
    maxAllocationSizeAcc.accumulate(currentAllocationSize.longValue());
    currentAllocationSize.addAndGet(-size);
    buffersDeallocatedAdder.increment();
    impl.free(buffer);
  }

  public long bytesAllocated() {
    return bytesAllocatedAdder.longValue();
  }

  public long bytesDeallocated() {
    return bytesDeallocatedAdder.longValue();
  }

  public long currentAllocation() {
    return currentAllocationSize.longValue();
  }

  public long maxAllocation() {
    return maxAllocationSizeAcc.longValue();
  }

  public long buffersAllocated() {
    return buffersAllocatedAdder.longValue();
  }

  public long buffersDeallocated() {
    return buffersDeallocatedAdder.longValue();
  }
}
