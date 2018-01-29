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

package com.mongodb.event;

import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterId;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * An event signifying that the cluster description has changed.
 *
 * @since 3.3
 */
public final class ClusterDescriptionChangedEvent {
    private final ClusterId clusterId;
    private final ClusterDescription newDescription;
    private final ClusterDescription previousDescription;

    /**
     * Constructs a new instance of the event.
     *
     * @param clusterId           the non-null cluster id
     * @param newDescription      the non-null new cluster description
     * @param previousDescription the non-null previous cluster description
     */
    public ClusterDescriptionChangedEvent(final ClusterId clusterId, final ClusterDescription newDescription,
                                          final ClusterDescription previousDescription) {
        this.clusterId = notNull("clusterId", clusterId);
        this.newDescription = notNull("newDescription", newDescription);
        this.previousDescription = notNull("previousDescription", previousDescription);
    }

    /**
     * Gets the cluster id associated with this event.
     *
     * @return the cluster id
     */
    public ClusterId getClusterId() {
        return clusterId;
    }

    /**
     * Gets the new cluster description.
     *
     * @return the cluster description
     */
    public ClusterDescription getNewDescription() {
        return newDescription;
    }

    /**
     * Gets the previous cluster description.
     *
     * @return the previous cluster description
     */
    public ClusterDescription getPreviousDescription() {
        return previousDescription;
    }

    @Override
    public String toString() {
        return "ClusterDescriptionChangedEvent{"
                       + "clusterId=" + clusterId
                       + ", newDescription=" + newDescription
                       + ", previousDescription=" + previousDescription
                       + '}';
    }
}
