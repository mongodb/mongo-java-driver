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

package com.mongodb.management;

/**
 * <p>A standard MXBean interface for a Mongo connection pool.</p>
 *
 * <p>This interface is NOT part of the public API.  Be prepared for non-binary compatible changes in minor releases.</p>
 *
 * @since 2.12
 */
public interface ConnectionPoolStatisticsMBean {

    /**
     * Gets the host that this connection pool is connecting to.
     *
     * @return the host
     */
    String getHost();

    /**
     * Gets the port that this connection pool is connecting to.
     *
     * @return the port
     */
    int getPort();

    /**
     * Gets the minimum allowed size of the pool, including idle and in-use members.
     *
     * @return the minimum size
     */
    int getMinSize();

    /**
     * Gets the maximum allowed size of the pool, including idle and in-use members.
     *
     * @return the maximum size
     */
    int getMaxSize();

    /**
     * Gets the current size of the pool, including idle and and in-use members.
     *
     * @return the size
     */
    int getSize();

    /**
     * Gets the count of connections that are currently in use.
     *
     * @return count of in-use connections
     */
    int getCheckedOutCount();
}
