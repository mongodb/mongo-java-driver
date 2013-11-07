/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb;

import org.mongodb.connection.ClusterDescription;

/**
 * An exception indicating that this version of the driver is not compatible with at least one of the servers that it is currently
 * connected to.
 *
 * @since 3.0.0
 */
public class MongoIncompatibleDriverException extends MongoException {
    private static final long serialVersionUID = -471605753882804308L;
    private final ClusterDescription clusterDescription;

    /**
     * Construct an instance with the given message and the description of the cluster which is incompatible.
     *
     * @param message the message
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
