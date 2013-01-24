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
 *
 */

package org.mongodb;

import org.mongodb.rs.ReplicaSet;

/**
 * An exception indicating a failure to find a replica set member that satisfies the requested read preference.
 */
public class MongoReadPreferenceException extends MongoClientException {
    private static final long serialVersionUID = 1L;

    private final ReadPreference readPreference;
    private final ReplicaSet replicaSet;

    /**
     * Construct a new instance with the given read preference and replica set.
     * @param readPreference the read preference
     * @param replicaSet the replica set
     */
    public MongoReadPreferenceException(final ReadPreference readPreference, final ReplicaSet replicaSet) {
        super(String.format("Unable to find a replica set member in %s that satisfies a read preference of %s",
                replicaSet, readPreference));
        this.readPreference = readPreference;
        this.replicaSet = replicaSet;
    }

    /**
     * Gets the requested read preference.
     *
     * @return the read preference
     */
    public ReadPreference getReadPreference() {
        return readPreference;
    }

    /**
     * Gets the replica set state at the time the operation was performed.
     *
     * @return the replica set state
     */
    public ReplicaSet getReplicaSet() {
        return replicaSet;
    }
}
