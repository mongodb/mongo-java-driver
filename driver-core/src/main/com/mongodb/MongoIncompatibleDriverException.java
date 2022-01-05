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

package com.mongodb;

import com.mongodb.connection.ClusterDescription;

/**
 * An exception indicating that this version of the driver is not compatible with at least one of the servers that it is currently
 * connected to.
 *
 * @since 2.12.0
 * @serial exclude
 */
public class MongoIncompatibleDriverException extends MongoException {
    private static final long serialVersionUID = -5213381354402601890L;
    private ClusterDescription clusterDescription;

    /**
     * Construct a new instance.
     *
     * @param message the error message
     * @param clusterDescription the cluster description
     */
    public MongoIncompatibleDriverException(final String message, final ClusterDescription clusterDescription) {
        super(message);
        this.clusterDescription = clusterDescription;
    }

    /**
     * The cluster description which was determined to be incompatible.
     *
     * @return the cluster description
     */
    public ClusterDescription getClusterDescription() {
        return clusterDescription;
    }

}
