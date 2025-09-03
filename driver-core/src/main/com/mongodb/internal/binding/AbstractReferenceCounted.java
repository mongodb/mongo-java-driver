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
 */

package com.mongodb.internal.binding;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public abstract class AbstractReferenceCounted implements ReferenceCounted {
    private final AtomicInteger referenceCount = new AtomicInteger(1);
    @Override
    public int getCount() {
        return referenceCount.get();
    }

    @Override
    public ReferenceCounted retain() {
        if (referenceCount.incrementAndGet() == 1) {
            throw new IllegalStateException("Attempted to increment the reference count when it is already 0");
        }
        return this;
    }

    @Override
    public int release() {
        int decrementedValue = referenceCount.decrementAndGet();
        if (decrementedValue < 0) {
            throw new IllegalStateException("Attempted to decrement the reference count below 0");
        }
        return decrementedValue;
    }
}
