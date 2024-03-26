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

package com.mongodb.client.model;


import com.mongodb.MongoClientSettings;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * The options to apply to the command when dropping indexes.
 *
 * @mongodb.driver.manual reference/command/dropIndexes/ Drop indexes
 * @since 3.6
 */
public class DropIndexOptions {
    private long maxTimeMS;

    /**
     * Gets the maximum execution time on the server for this operation.  The default is 0, which places no limit on the execution time.
     *
     * @param timeUnit the time unit to return the result in
     * @return the maximum execution time in the given time unit
     * @deprecated Prefer using the operation execution timeout configuration options available at the following levels:
     *
     * <ul>
     *     <li>{@link MongoClientSettings.Builder#getTimeout(TimeUnit)}</li>
     *     <li>{@code MongoDatabase#getTimeout(TimeUnit)}</li>
     *     <li>{@code MongoCollection#getTimeout(TimeUnit)}</li>
     *     <li>{@link com.mongodb.ClientSessionOptions}</li>
     *     <li>{@link com.mongodb.TransactionOptions}</li>
     * </ul>
     *
     * When executing an operation, any explicitly set timeout at these levels takes precedence, rendering this maximum execution time
     * irrelevant. If no timeout is specified at these levels, the maximum execution time will be used.
     */
    @Deprecated
    public long getMaxTime(final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        return timeUnit.convert(maxTimeMS, TimeUnit.MILLISECONDS);
    }

    /**
     * Sets the maximum execution time on the server for this operation.
     *
     * @param maxTime  the max time
     * @param timeUnit the time unit, which may not be null
     * @return this
     * @deprecated Prefer using the operation execution timeout configuration options available at the following levels:
     *
     * <ul>
     *     <li>{@link MongoClientSettings.Builder#timeout(long, TimeUnit)}</li>
     *     <li>{@code  MongoDatabase#withTimeout(long, TimeUnit)}</li>
     *     <li>{@code  MongoCollection#withTimeout(long, TimeUnit)}</li>
     *     <li>{@link com.mongodb.ClientSessionOptions}</li>
     *     <li>{@link com.mongodb.TransactionOptions}</li>
     * </ul>
     *
     * When executing an operation, any explicitly set timeout at these levels takes precedence, rendering this maximum execution time
     * irrelevant. If no timeout is specified at these levels, the maximum execution time will be used.
     */
    @Deprecated
    public DropIndexOptions maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    @Override
    public String toString() {
        return "DropIndexOptions{"
                + "maxTimeMS=" + maxTimeMS
                + '}';
    }
}
