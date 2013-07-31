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
package com.mongodb.util;

import com.mongodb.InUseConnectionBean;

/**
 * A bean representing connection pool statistics.
 *
 * @deprecated This class will be removed in 3.x versions of the driver,
 *             so please remove it from your compile time dependencies.
 */
@Deprecated
public class ConnectionPoolStatisticsBean {
    private final int total;
    private final int inUse;
    private final InUseConnectionBean[] inUseConnections;

    public ConnectionPoolStatisticsBean(final int total, final int inUse, final InUseConnectionBean[] inUseConnections) {
        //To change body of created methods use File | Settings | File Templates.
        this.total = total;
        this.inUse = inUse;
        this.inUseConnections = inUseConnections;
    }

    /**
     * Gets the total number of pool members, including idle and and in-use members.
     *
     * @return total number of members
     */
    public int getTotal() {
        return total;
    }

    /**
     * Gets the number of pool members that are currently in use.
     *
     * @return number of in-use members
     */
    public int getInUse() {
        return inUse;
    }

    /**
     * Gets an array of beans describing all the connections that are currently in use.
     *
     * @return array of in-use connection beans
     */
    public InUseConnectionBean[] getInUseConnections() {
        return inUseConnections;
    }
}
