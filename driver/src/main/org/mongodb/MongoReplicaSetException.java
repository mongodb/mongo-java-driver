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

public class MongoReplicaSetException extends MongoClientException {

    private static final long serialVersionUID = 6557484831390294566L;

    private final ReplicaSetDescription replicaSetDescription;

    public MongoReplicaSetException(final String msg, final ReplicaSetDescription replicaSetDescription) {
        super(msg);
        this.replicaSetDescription = replicaSetDescription;
    }

    /**
     * Gets the replica set state at the time the operation was performed.
     *
     * @return the replica set state
     */
    public ReplicaSetDescription getReplicaSetDescription() {
        return replicaSetDescription;
    }
}
