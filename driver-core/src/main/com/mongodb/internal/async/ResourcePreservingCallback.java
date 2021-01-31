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

package com.mongodb.internal.async;

import com.mongodb.internal.binding.ReferenceCounted;
import com.mongodb.lang.Nullable;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * A decorator that {@linkplain ReferenceCounted#retain() retains} the supplied resource in the constructor
 * and idempotently {@linkplain ReferenceCounted#release() releases} it in {@link #onResult(Object, Throwable)}.
 * <p>
 * This class is not part of the public API and may be removed or changed at any time.
 *
 * @param <T> the result type
 */
public final class ResourcePreservingCallback<T> implements SingleResultCallback<T> {
    @SuppressWarnings("rawtypes")
    private static final AtomicReferenceFieldUpdater<ResourcePreservingCallback, ReferenceCounted> RESOURCE =
            AtomicReferenceFieldUpdater.newUpdater(ResourcePreservingCallback.class, ReferenceCounted.class, "resource");

    @Nullable
    private volatile ReferenceCounted resource;
    @Nullable
    private final SingleResultCallback<? super T> wrapped;

    private ResourcePreservingCallback(final ReferenceCounted resource, @Nullable final SingleResultCallback<? super T> wrapped) {
        this.resource = notNull("resource must not be null", resource);
        resource.retain();
        this.wrapped = wrapped;
    }

    public static <T> SingleResultCallback<T> resourcePreservingCallback(
            final ReferenceCounted resource, @Nullable final SingleResultCallback<T> wrapped) {
        notNull("resource must not be null", resource);
        return new ResourcePreservingCallback<>(resource, wrapped);
    }

    @Override
    public void onResult(final T result, final Throwable t) {
        try {
            releaseResourceIdempotently();
        } finally {
            if (wrapped != null) {
                wrapped.onResult(result, t);
            }
        }
    }

    private void releaseResourceIdempotently() {
        ReferenceCounted localResource = RESOURCE.getAndSet(this, null);
        if (localResource != null) {
            localResource.release();
        }
    }
}
