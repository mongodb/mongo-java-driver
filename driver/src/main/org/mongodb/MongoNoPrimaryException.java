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

import org.mongodb.rs.ReplicaSetDescription;

/**
 * An exception indicating a failure to find a member of the replica set that is the current primary.
 */
public class MongoNoPrimaryException extends MongoReplicaSetException {

    private static final long serialVersionUID = -3436794277396256069L;

    /**
     * Constructs a new instance with the given replica set state.
     * @param replicaSetDescription the replica set state
     */
    public MongoNoPrimaryException(final ReplicaSetDescription replicaSetDescription) {
        super(String.format("The MongoClient is unable to find a primary member of the replica set: %s", replicaSetDescription),
                replicaSetDescription);
    }
}
