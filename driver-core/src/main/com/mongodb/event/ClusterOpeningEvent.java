/*
 * Copyright 2008-2016 MongoDB, Inc.
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

import com.mongodb.connection.ClusterId;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * A cluster opening event.
 *
 * @since 3.3
 */
public final class ClusterOpeningEvent {

    private final ClusterId clusterId;

    /**
     * Constructs a new instance of the event.
     *
     * @param clusterId the cluster id
     */
    public ClusterOpeningEvent(final ClusterId clusterId) {
        this.clusterId = notNull("clusterId", clusterId);
    }


    /***
     * Gets the cluster id.
     *
     * @return the cluster id
     */
    public ClusterId getClusterId() {
        return clusterId;
    }

    @Override
    public String toString() {
        return "ClusterOpeningEvent{"
                       + "clusterId=" + clusterId
                       + '}';
    }
}

