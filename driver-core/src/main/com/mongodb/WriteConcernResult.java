/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

import org.bson.BsonValue;

/**
 * The result of a successful write operation.  If the write was unacknowledged, then {@code wasAcknowledged} will return false and all
 * other methods with throw {@code MongoUnacknowledgedWriteException}.
 *
 * @see com.mongodb.WriteConcern#UNACKNOWLEDGED
 * @since 3.0
 */
public interface WriteConcernResult {

    /**
     * Returns true if the write was acknowledged.
     *
     * @return true if the write was acknowledged
     */
    boolean wasAcknowledged();

    /**
     * Returns the number of documents affected by the write operation.
     *
     * @return the number of documents affected by the write operation
     * @throws com.mongodb.UnacknowledgedWriteException if the write was unacknowledged.
     */
    int getCount();

    /**
     * Returns true if the write was an update of an existing document.
     *
     * @return true if the write was an update of an existing document
     * @throws com.mongodb.UnacknowledgedWriteException if the write was unacknowledged.
     */
    boolean isUpdateOfExisting();

    /**
     * Returns the value of _id if this write resulted in an upsert.
     *
     * @return the value of _id if this write resulted in an upsert.
     * @throws com.mongodb.UnacknowledgedWriteException if the write was unacknowledged.
     */
    BsonValue getUpsertedId();
}
