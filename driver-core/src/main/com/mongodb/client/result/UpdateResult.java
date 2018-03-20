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

package com.mongodb.client.result;

import com.mongodb.lang.Nullable;
import org.bson.BsonValue;

/**
 * The result of an update operation.  If the update was unacknowledged, then {@code wasAcknowledged} will return false and all other
 * methods will throw {@code UnsupportedOperationException}.
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
     * Gets a value indicating whether the modified count is available.
     * <p>
     * The modified count is only available when all servers have been upgraded to 2.6 or above.
     * </p>
     * @return true if the modified count is available
     */
    public abstract boolean isModifiedCountAvailable();

    /**
     * Gets the number of documents modified by the update.
     *
     * @return the number of documents modified
     */
    public abstract long getModifiedCount();

    /**
     * If the replace resulted in an inserted document, gets the _id of the inserted document, otherwise null.
     *
     * @return if the replace resulted in an inserted document, the _id of the inserted document, otherwise null
     */
    @Nullable
    public abstract BsonValue getUpsertedId();

    /**
     * Create an acknowledged UpdateResult
     *
     * @param matchedCount  the number of documents matched
     * @param modifiedCount the number of documents modified
     * @param upsertedId    if the replace resulted in an inserted document, the id of the inserted document
     * @return an acknowledged UpdateResult
     */
    public static UpdateResult acknowledged(final long matchedCount, @Nullable final Long modifiedCount,
                                            @Nullable final BsonValue upsertedId) {
        return new AcknowledgedUpdateResult(matchedCount, modifiedCount, upsertedId);
    }

    /**
     * Create an unacknowledged UpdateResult
     *
     * @return an unacknowledged UpdateResult
     */
    public static UpdateResult unacknowledged() {
        return new UnacknowledgedUpdateResult();
    }

    private static class AcknowledgedUpdateResult extends UpdateResult {
        private final long matchedCount;
        private final Long modifiedCount;
        private final BsonValue upsertedId;

        AcknowledgedUpdateResult(final long matchedCount, @Nullable final Long modifiedCount, @Nullable final BsonValue upsertedId) {
            this.matchedCount = matchedCount;
            this.modifiedCount = modifiedCount;
            this.upsertedId = upsertedId;
        }

        @Override
        public boolean wasAcknowledged() {
            return true;
        }

        @Override
        public long getMatchedCount() {
            return matchedCount;
        }

        @Override
        public boolean isModifiedCountAvailable() {
            return modifiedCount != null;
        }

        @Override
        public long getModifiedCount() {
            if (modifiedCount == null) {
                throw new UnsupportedOperationException("Modified count is only available when connected to MongoDB 2.6 servers or "
                                                        + "above.");
            }
            return modifiedCount;
        }

        @Override
        @Nullable
        public BsonValue getUpsertedId() {
            return upsertedId;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            AcknowledgedUpdateResult that = (AcknowledgedUpdateResult) o;

            if (matchedCount != that.matchedCount) {
                return false;
            }
            if (modifiedCount != null ? !modifiedCount.equals(that.modifiedCount) : that.modifiedCount != null) {
                return false;
            }
            if (upsertedId != null ? !upsertedId.equals(that.upsertedId) : that.upsertedId != null) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            int result = (int) (matchedCount ^ (matchedCount >>> 32));
            result = 31 * result + (modifiedCount != null ? modifiedCount.hashCode() : 0);
            result = 31 * result + (upsertedId != null ? upsertedId.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "AcknowledgedUpdateResult{"
                   + "matchedCount=" + matchedCount
                   + ", modifiedCount=" + modifiedCount
                   + ", upsertedId=" + upsertedId
                   + '}';
        }
    }

    private static class UnacknowledgedUpdateResult extends UpdateResult {
        @Override
        public boolean wasAcknowledged() {
            return false;
        }

        @Override
        public long getMatchedCount() {
            throw getUnacknowledgedWriteException();
        }

        @Override
        public boolean isModifiedCountAvailable() {
            return false;
        }

        @Override
        public long getModifiedCount() {
            throw getUnacknowledgedWriteException();
        }

        @Override
        @Nullable
        public BsonValue getUpsertedId() {
           throw getUnacknowledgedWriteException();
        }

        private UnsupportedOperationException getUnacknowledgedWriteException() {
            return new UnsupportedOperationException("Cannot get information about an unacknowledged update");
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            return 0;
        }

        @Override
        public String toString() {
            return "UnacknowledgedUpdateResult{}";
        }
    }
}
