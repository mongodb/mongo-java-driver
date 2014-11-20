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

package com.mongodb.client.result;

import com.mongodb.UnacknowledgedWriteException;
import org.bson.BsonValue;

/**
 * The result of an update operation.  If the update was unacknowledged, then {@code wasAcknowledged} will return false and all other
 * methods with throw {@code MongoUnacknowledgedWriteException}.
 *
 * @see com.mongodb.WriteConcern#UNACKNOWLEDGED
 * @since 3.0
 */
public abstract class UpdateResult {

    /**
     * Returns true if the write was acknowledged.
     *
     * @return true if the write was acknowledged
     */
    public abstract boolean wasAcknowledged();

    /**
     * Gets the number of documents matched by the query.
     *
     * @return the number of documents matched
     */
    public abstract long getMatchedCount();

    /**
     * Gets he number of documents modified by the update.
     *
     * @return the number of documents modified
     */
    public abstract long getModifiedCount();

    /**
     * If the replace resulted in an inserted document, gets the _id of the inserted document, otherwise null.
     *
     * @return if the replace resulted in an inserted document, the _id of the inserted document, otherwise null
     */
    public abstract BsonValue getUpsertedId();

    /**
     * Create an acknowledged UpdateResult
     *
     * @param matchedCount  the number of documents matched
     * @param modifiedCount the number of documents modified
     * @param upsertedId    if the replace resulted in an inserted document, the id of the inserted document
     * @return an acknowledged UpdateResult
     */
    public static UpdateResult acknowledged(final long matchedCount, final long modifiedCount, final BsonValue upsertedId) {
        return new UpdateResult() {
            @Override
            public boolean wasAcknowledged() {
                return true;
            }

            @Override
            public long getMatchedCount() {
                return matchedCount;
            }

            @Override
            public long getModifiedCount() {
                return modifiedCount;
            }

            @Override
            public BsonValue getUpsertedId() {
                return upsertedId;
            }
        };
    }

    /**
     * Create an unacknowledged UpdateResult
     *
     * @return an unacknowledged UpdateResult
     */
    public static UpdateResult unacknowledged() {
        return new UpdateResult() {
            @Override
            public boolean wasAcknowledged() {
                return false;
            }

            @Override
            public long getMatchedCount() {
                throw getUnacknowledgedWriteException();
            }

            @Override
            public long getModifiedCount() {
                throw getUnacknowledgedWriteException();
            }

            @Override
            public BsonValue getUpsertedId() {
               throw getUnacknowledgedWriteException();
            }

            private UnacknowledgedWriteException getUnacknowledgedWriteException() {
                return new UnacknowledgedWriteException("Cannot get information about an unacknowledged update");
            }
        };
    }

}
