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

package com.mongodb.bulk;

import com.mongodb.internal.bulk.WriteRequest;

import java.util.List;

/**
 * The result of a successful bulk write operation.
 *
 * @since 3.0
 */
public abstract class BulkWriteResult {

    /**
     * Returns true if the write was acknowledged.
     *
     * @return true if the write was acknowledged
     * @see com.mongodb.WriteConcern#UNACKNOWLEDGED
     */
    public abstract boolean wasAcknowledged();

    /**
     * Returns the number of documents inserted by the write operation.
     *
     * @return the number of documents inserted by the write operation
     * @throws java.lang.UnsupportedOperationException if the write was unacknowledged.
     * @see com.mongodb.WriteConcern#UNACKNOWLEDGED
     */
    public abstract int getInsertedCount();

    /**
     * Returns the number of documents matched by updates or replacements in the write operation.  This will include documents that matched
     * the query but where the modification didn't result in any actual change to the document; for example, if you set the value of some
     * field, and the field already has that value, that will still count as an update.
     *
     * @return the number of documents matched by updates in the write operation
     * @throws java.lang.UnsupportedOperationException if the write was unacknowledged.
     * @see com.mongodb.WriteConcern#UNACKNOWLEDGED
     */
    public abstract int getMatchedCount();

    /**
     * Returns the number of documents deleted by the write operation.
     *
     * @return the number of documents deleted by the write operation
     * @throws java.lang.UnsupportedOperationException if the write was unacknowledged.
     * @see com.mongodb.WriteConcern#UNACKNOWLEDGED
     */
    public abstract int getDeletedCount();

    /**
     * Returns true if the server was able to provide a count of modified documents.
     *
     * <p>
     * This method now always returns true, as modified count is available since MongoDB 2.6.
     * </p>
     * @return true if modifiedCount is available
     * @throws java.lang.UnsupportedOperationException if the write was unacknowledged.
     * @see com.mongodb.WriteConcern#UNACKNOWLEDGED
     * @see #getModifiedCount()
     * @deprecated no longer needed since all supported server versions support modified count
     */
    @Deprecated
    public abstract boolean isModifiedCountAvailable();

    /**
     * Returns the number of documents modified by the write operation.  This only applies to updates or replacements, and will only count
     * documents that were actually changed; for example, if you set the value of some field , and the field already has that value, that
     * will not count as a modification.
     *
     * @return the number of documents modified by the write operation
     * @see com.mongodb.WriteConcern#UNACKNOWLEDGED
     */
    public abstract int getModifiedCount();


    /**
     * Gets an unmodifiable list of upserted items, or the empty list if there were none.
     *
     * @return a list of upserted items, or the empty list if there were none.
     * @throws java.lang.UnsupportedOperationException if the write was unacknowledged.
     * @see com.mongodb.WriteConcern#UNACKNOWLEDGED
     */
    public abstract List<BulkWriteUpsert> getUpserts();

    /**
     * Create an acknowledged BulkWriteResult
     *
     * @param type    the type of the write
     * @param count   the number of documents matched
     * @param upserts the list of upserts
     * @return an acknowledged BulkWriteResult
     */
    public static BulkWriteResult acknowledged(final WriteRequest.Type type, final int count, final List<BulkWriteUpsert> upserts) {
        return acknowledged(type, count, 0, upserts);
    }

    /**
     * Create an acknowledged BulkWriteResult
     *
     * @param type          the type of the write
     * @param count         the number of documents matched
     * @param modifiedCount the number of documents modified, which may be null if the server was not able to provide the count
     * @param upserts       the list of upserts
     * @return an acknowledged BulkWriteResult
     */
    public static BulkWriteResult acknowledged(final WriteRequest.Type type, final int count, final Integer modifiedCount,
                                               final List<BulkWriteUpsert> upserts) {
        return acknowledged(type == WriteRequest.Type.INSERT ? count : 0,
                            (type == WriteRequest.Type.UPDATE || type == WriteRequest.Type.REPLACE) ? count : 0,
                            type == WriteRequest.Type.DELETE ? count : 0,
                            modifiedCount, upserts);
    }

    /**
     * Create an acknowledged BulkWriteResult
     *
     * @param insertedCount the number of documents inserted by the write operation
     * @param matchedCount  the number of documents matched by the write operation
     * @param removedCount  the number of documents removed by the write operation
     * @param modifiedCount the number of documents modified, which may not be null
     * @param upserts       the list of upserts
     * @return an acknowledged BulkWriteResult
     */
    public static BulkWriteResult acknowledged(final int insertedCount, final int matchedCount, final int removedCount,
                                               final Integer modifiedCount, final List<BulkWriteUpsert> upserts) {
        return new BulkWriteResult() {
            @Override
            public boolean wasAcknowledged() {
                return true;
            }

            @Override
            public int getInsertedCount() {
                return insertedCount;
            }

            @Override
            public int getMatchedCount() {
                return matchedCount;
            }

            @Override
            public int getDeletedCount() {
                return removedCount;
            }

            @Override
            @Deprecated
            public boolean isModifiedCountAvailable() {
                return true;
            }

            @Override
            public int getModifiedCount() {
                return modifiedCount;
            }

            @Override
            public List<BulkWriteUpsert> getUpserts() {
                return upserts;
            }

            @Override
            public boolean equals(final Object o) {
                if (this == o) {
                    return true;
                }
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }

                BulkWriteResult that = (BulkWriteResult) o;

                if (!that.wasAcknowledged()) {
                    return false;
                }
                if (insertedCount != that.getInsertedCount()) {
                    return false;
                }
                if (modifiedCount != null && !modifiedCount.equals(that.getModifiedCount())) {
                    return false;
                }
                if (removedCount != that.getDeletedCount()) {
                    return false;
                }
                if (matchedCount != that.getMatchedCount()) {
                    return false;
                }
                if (!upserts.equals(that.getUpserts())) {
                    return false;
                }

                return true;
            }

            @Override
            public int hashCode() {
                int result = upserts.hashCode();
                result = 31 * result + insertedCount;
                result = 31 * result + matchedCount;
                result = 31 * result + removedCount;
                result = 31 * result + (modifiedCount != null ? modifiedCount.hashCode() : 0);
                return result;
            }

            @Override
            public String toString() {
                return "AcknowledgedBulkWriteResult{"
                       + "insertedCount=" + insertedCount
                       + ", matchedCount=" + matchedCount
                       + ", removedCount=" + removedCount
                       + ", modifiedCount=" + modifiedCount
                       + ", upserts=" + upserts
                       + '}';
            }
        };
    }

    /**
     * Create an unacknowledged BulkWriteResult
     *
     * @return an unacknowledged BulkWriteResult
     */
    public static BulkWriteResult unacknowledged() {
        return new BulkWriteResult() {
            @Override
            public boolean wasAcknowledged() {
                return false;
            }

            @Override
            public int getInsertedCount() {
                throw getUnacknowledgedWriteException();
            }

            @Override
            public int getMatchedCount() {
                throw getUnacknowledgedWriteException();
            }

            @Override
            public int getDeletedCount() {
                throw getUnacknowledgedWriteException();
            }

            @Override
            @Deprecated
            public boolean isModifiedCountAvailable() {
                throw getUnacknowledgedWriteException();
            }

            @Override
            public int getModifiedCount() {
                throw getUnacknowledgedWriteException();
            }

            @Override
            public List<BulkWriteUpsert> getUpserts() {
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

                BulkWriteResult that = (BulkWriteResult) o;
                return !that.wasAcknowledged();
            }

            @Override
            public int hashCode() {
                return 0;
            }

            @Override
            public String toString() {
                return "UnacknowledgedBulkWriteResult{}";
            }

            private UnsupportedOperationException getUnacknowledgedWriteException() {
                return new UnsupportedOperationException("Cannot get information about an unacknowledged write");
            }
        };
    }

}
