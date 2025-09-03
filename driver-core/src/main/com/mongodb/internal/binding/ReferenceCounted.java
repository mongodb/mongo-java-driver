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

import com.mongodb.internal.VisibleForTesting;

import static com.mongodb.internal.VisibleForTesting.AccessModifier.PRIVATE;

/**
 * An interface for reference-counted objects.
 * <p>
 * The recommended usage pattern:
 * <pre>{@code
 * ReferenceCounted resource = new ...;
 * //there is no need to call resource.retain() as ReferenceCounted objects are created as retained (the getCount method returns 1)
 * try {
 *     //Use the resource.
 *     //If the resource is passed as a method argument,
 *     //it is the responsibility of the receiver to call the retain and the corresponding release methods,
 *     //if the receiver stores the resource for later use.
 * } finally {
 *     resource.release();
 * }
 * }</pre>
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public interface ReferenceCounted {
    /**
     * Gets the current reference count.
     *
     * <p>
     * This method should only be used for testing.  Production code should prefer using the count returned from {@link #release()}
     * </p>
     *
     * @return the current count, which must be greater than or equal to 0.
     * Returns 1 for a newly created object.
     */
    @VisibleForTesting(otherwise = PRIVATE)
    int getCount();

    /**
     * Retain an additional reference to this object.  All retained references must be released, or there will be a leak.
     *
     * @return this
     */
    ReferenceCounted retain();

    /**
     * Release a reference to this object.
     * @throws java.lang.IllegalStateException if the reference count is already 0
     * @return the reference count after the release
     */
    int release();
}
