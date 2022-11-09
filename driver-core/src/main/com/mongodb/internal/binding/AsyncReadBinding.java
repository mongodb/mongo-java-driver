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

import com.mongodb.ReadPreference;
import com.mongodb.RequestContext;
import com.mongodb.ServerApi;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.lang.Nullable;

/**
 * An asynchronous factory of connection sources to servers that can be read from and that satisfy the specified read preference.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public interface AsyncReadBinding extends ReferenceCounted {
    /**
     * The read preference that all connection sources returned by this instance will satisfy.
     * @return the non-null read preference
     */
    ReadPreference getReadPreference();

    /**
     * Gets the session context for this binding.
     *
     * @return the session context, which may not be null
     *
     */
    SessionContext getSessionContext();

    @Nullable
    ServerApi getServerApi();

    RequestContext getRequestContext();

    /**
     * Returns a connection source to a server that satisfies the read preference with which this instance is configured.
     * @param callback the to be passed the connection source
     */
    void getReadConnectionSource(SingleResultCallback<AsyncConnectionSource> callback);

    /**
     * Return a connection source that satisfies the read preference with which this instance is configured, if all connected servers have
     * a maxWireVersion >= the given minWireVersion.  Otherwise, return a connection source that satisfied the given
     * fallbackReadPreference.
     * <p>
     * This is useful for operations that are able to execute on a secondary on later server versions, but must execute on the primary on
     * earlier server versions.
     *
     * @see com.mongodb.internal.operation.AggregateToCollectionOperation
     */
    void getReadConnectionSource(int minWireVersion, ReadPreference fallbackReadPreference,
            SingleResultCallback<AsyncConnectionSource> callback);

    @Override
    AsyncReadBinding retain();
}
