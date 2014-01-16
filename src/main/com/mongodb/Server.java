/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb;

import org.bson.util.annotations.ThreadSafe;

import java.util.concurrent.TimeUnit;

/**
 * A logical connection to a MongoDB server.
 */
@ThreadSafe
interface Server {
    /**
     * Gets the description of this server.  Implementations of this method should not block if the server has not yet been successfully
     * contacted, but rather return immediately a @code{ServerDescription} in a @code{ServerDescription.Status.Connecting} state.
     *
     * @return the description of this server
     */
    ServerDescription getDescription();

    Connection getConnection(final long maxWaitTime, final TimeUnit timeUnit);

    void invalidate();
}
