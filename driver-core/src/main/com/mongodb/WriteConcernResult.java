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

package com.mongodb;

import org.bson.BsonValue;

/**
 * The result of a successful write operation.  If the write was unacknowledged, then {@code wasAcknowledged} will return false and all
 * other methods will throw {@code MongoUnacknowledgedWriteException}.
 *
 * @see com.mongodb.WriteConcern#UNACKNOWLEDGED
 * @since 3.0
 */
public abstract class WriteConcernResult {

    /**
     * Returns true if the write was acknowledged.
     *
     * @return true if the write was acknowledged
     */
    public abstract boolean wasAcknowledged();

    /**
     * Returns the number of documents affected by the write operation.
     *
     * @return the number of documents affected by the write operation
     * @throws UnsupportedOperationException if the write was unacknowledged.
     */
    public abstract int getCount();

    /**
     * Returns true if the write was an update of an existing document.
     *
     * @return true if the write was an update of an existing document
     * @throws UnsupportedOperationException if the write was unacknowledged.
     */
    public abstract boolean isUpdateOfExisting();

    /**
     * Returns the value of _id if this write resulted in an upsert.
     *
     * @return the value of _id if this write resulted in an upsert.
     * @throws UnsupportedOperationException if the write was unacknowledged.
     */
    public abstract BsonValue getUpsertedId();

    /**
     * Create an acknowledged WriteConcernResult
     *
     * @param count the count of matched documents
     * @param isUpdateOfExisting whether an existing document was updated
     * @param upsertedId if an upsert resulted in an inserted document, this is the _id of that document.  This may be null
     * @return an acknowledged WriteConcernResult
     */
    public static WriteConcernResult acknowledged(final int count, final boolean isUpdateOfExisting, final BsonValue upsertedId) {
        return new WriteConcernResult() {
            @Override
            public boolean wasAcknowledged() {
                return true;
            }

            @Override
            public int getCount() {
                return count;
            }

            @Override
            public boolean isUpdateOfExisting() {
                return isUpdateOfExisting;
            }

            @Override
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

                WriteConcernResult that = (WriteConcernResult) o;
                if (!that.wasAcknowledged()) {
                    return false;
                }

                if (count != that.getCount()) {
                    return false;
                }
                if (isUpdateOfExisting != that.isUpdateOfExisting()) {
                    return false;
                }
                if (upsertedId != null ? !upsertedId.equals(that.getUpsertedId()) : that.getUpsertedId() != null) {
                    return false;
                }

                return true;
            }

            @Override
            public int hashCode() {
                int result = count;
                result = 31 * result + (isUpdateOfExisting ? 1 : 0);
                result = 31 * result + (upsertedId != null ? upsertedId.hashCode() : 0);
                return result;
            }

            @Override
            public String toString() {
                return "AcknowledgedWriteResult{"
                       + "count=" + count
                       + ", isUpdateOfExisting=" + isUpdateOfExisting
                       + ", upsertedId=" + upsertedId
                       + '}';
            }
        };
    }

    /**
     * Create an unacknowledged WriteConcernResult
     *
     * @return an unacknowledged WriteConcernResult
     */
    public static WriteConcernResult unacknowledged() {
        return new WriteConcernResult() {
            @Override
            public boolean wasAcknowledged() {
                return false;
            }

            @Override
            public int getCount() {
                throw getUnacknowledgedWriteException();
            }

            @Override
            public boolean isUpdateOfExisting() {
                throw getUnacknowledgedWriteException();
            }

            @Override
            public BsonValue getUpsertedId() {
                throw getUnacknowledgedWriteException();
            }

            @Override
            public boolean equals(final Object o) {
                if (this == o) {
                    return true;
                }
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }

                WriteConcernResult that = (WriteConcernResult) o;
                return !that.wasAcknowledged();
            }

            @Override
            public int hashCode() {
                return 1;
            }

            @Override
            public String toString() {
                return "UnacknowledgedWriteResult{}";
            }

            private UnsupportedOperationException getUnacknowledgedWriteException() {
                return new UnsupportedOperationException("Cannot get information about an unacknowledged write");
            }
        };
    }
}
