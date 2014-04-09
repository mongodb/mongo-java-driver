/*
 * Copyright (c) 2008 - 2014 MongoDB, Inc.
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

package org.mongodb.session;

import org.mongodb.selector.ServerSelector;

import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mongodb.assertions.Assertions.notNull;

/**
 * @since 3.0
 */
public class ServerConnectionProviderOptions {

    private final boolean isQuery;
    private final ServerSelector serverSelector;
    private long maxWaitTimeMS;

    public ServerConnectionProviderOptions(final boolean query, final ServerSelector serverSelector) {
        isQuery = query;
        this.serverSelector = notNull("serverSelector", serverSelector);
        maxWaitTimeMS = MILLISECONDS.convert(30, SECONDS);
    }

    public boolean isQuery() {
        return isQuery;
    }

    public ServerSelector getServerSelector() {
        return serverSelector;
    }

    /**
     * Gets the maximum amount of time to wait selecting a server from the cluster. The default is 30 seconds.
     *
     * @param timeUnit the time unit to get the max wait time in
     * @return the max wait time in the given time unit
     */
    public long getMaxWaitTime(final TimeUnit timeUnit) {
        return timeUnit.convert(maxWaitTimeMS, MILLISECONDS);
    }

    /**
     * Sets the maximum amount of time to wait selecting a server from the cluster.
     *
     * @param maxWaitTime the max wait time
     * @param timeUnit the time unit for the max wait time
     */
    public void setMaxWaitTime(final long maxWaitTime, final TimeUnit timeUnit){
        maxWaitTimeMS = MILLISECONDS.convert(maxWaitTime, timeUnit);
    }
}
