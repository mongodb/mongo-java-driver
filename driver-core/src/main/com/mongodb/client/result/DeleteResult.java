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

/**
 * The result of a delete operation. If the delete was unacknowledged, then {@code wasAcknowledged} will return false and all other methods
 * with throw {@code MongoUnacknowledgedWriteException}.
 *
 * @see com.mongodb.WriteConcern#UNACKNOWLEDGED
 * @since 3.0
 */
public abstract class DeleteResult {

    /**
     * Returns true if the write was acknowledged.
     *
     * @return true if the write was acknowledged
     */
    public abstract boolean wasAcknowledged();

    /**
     * Gets the number of documents deleted.
     *
     * @return the number of documents deleted
     */
    public abstract long getDeletedCount();


    /**
     * Create an acknowledged DeleteResult
     *
     * @param deletedCount  the number of documents deleted
     * @return an acknowledged DeleteResult
     */
    public static DeleteResult acknowledged(final long deletedCount) {
        return new DeleteResult() {
            @Override
            public boolean wasAcknowledged() {
                return true;
            }

            @Override
            public long getDeletedCount() {
                return deletedCount;
            }
        };
    }

    /**
     * Create an unacknowledged DeleteResult
     *
     * @return an unacknowledged DeleteResult
     */
    public static DeleteResult unacknowledged() {
        return new DeleteResult() {
            @Override
            public boolean wasAcknowledged() {
                return false;
            }

            @Override
            public long getDeletedCount() {
                throw new UnacknowledgedWriteException("Cannot get information about an unacknowledged delete");
            }
        };
    }

}
