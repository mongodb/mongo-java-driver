/**
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb;

/**
 * A standard MBean interface for a Mongo connection pool, for use on Java 5 virtual machines.
 * <p>
 * This interface is NOT part of the public API.  Be prepared for non-binary compatible changes in minor releases.
 *
 * @deprecated This class will be removed in 3.x versions of the driver,
 *             so please remove it from your compile time dependencies.
 */
@Deprecated
public interface Java5MongoConnectionPoolMBean {
    /**
     * Gets the name of the pool.
     *
     * @return the name of the pool
     */
    String getName();

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
     * Gets the total number of pool members, including idle and and in-use members.
     *
     * @return total number of members
     */
    int getTotal();

    /**
     * Gets the number of pool members that are currently in use.
     *
     * @return number of in-use members
     */
    int getInUse();

    /**
     * Gets the maximum allowed size of the pool, including idle and in-use members.
     *
     * @return the maximum size
     */
    int getMaxSize();
}
