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


import com.mongodb.lang.Nullable;

/**
 * This class lets you access the results of the previous acknowledged write. If the write was unacknowledged, all property access
 * methods will throw {@link UnsupportedOperationException}.
 *
 * @see WriteConcern#UNACKNOWLEDGED
 */
public class WriteResult {

    private final boolean acknowledged;
    private final int n;
    private final boolean updateOfExisting;
    private final Object upsertedId;

    /**
     * Gets an instance representing an unacknowledged write.
     *
     * @return an instance representing an unacknowledged write
     * @since 3.0
     */
    public static WriteResult unacknowledged() {
       return new WriteResult();
    }

    /**
     * Construct a new instance.
     *
     * @param n the number of existing documents affected by this operation
     * @param updateOfExisting true if the operation was an update and an existing document was updated
     * @param upsertedId the _id of a document that was upserted by this operation, which may be null
     */
    public WriteResult(final int n, final boolean updateOfExisting, @Nullable final Object upsertedId) {
        this.acknowledged = true;
        this.n = n;
        this.updateOfExisting = updateOfExisting;
        this.upsertedId = upsertedId;
    }

    WriteResult() {
        acknowledged = false;
        n = 0;
        updateOfExisting = false;
        upsertedId = null;
    }

    /**
     * Returns true if the write was acknowledged.
     *
     * @return true if the write was acknowledged
     * @see WriteConcern#UNACKNOWLEDGED
     * @since 3.0
     */
    public boolean wasAcknowledged() {
        return acknowledged;
    }

    /**
     * Gets the "n" field, which contains the number of documents affected in the write operation.
     *
     * @return the value of the "n" field
     * @throws UnsupportedOperationException if the write was unacknowledged
     * @see WriteConcern#UNACKNOWLEDGED
     */
    public int getN() {
        throwIfUnacknowledged("n");
        return n;
    }

    /**
     * Gets the _id value of an upserted document that resulted from this write.
     *
     * @return the value of the _id of an upserted document, which may be null
     * @throws UnsupportedOperationException if the write was unacknowledged
     * @since 2.12
     */
    @Nullable
    public Object getUpsertedId() {
        throwIfUnacknowledged("upsertedId");
        return upsertedId;
    }


    /**
     * Returns true if this write resulted in an update of an existing document.
     *
     * @return whether the write resulted in an update of an existing document.
     * @throws UnsupportedOperationException if the write was unacknowledged
     * @since 2.12
     */
    public boolean isUpdateOfExisting() {
        throwIfUnacknowledged("updateOfExisting");
        return updateOfExisting;
    }

    @Override
    public String toString() {
        if (acknowledged) {
            return "WriteResult{"
                   + "n=" + n
                   + ", updateOfExisting=" + updateOfExisting
                   + ", upsertedId=" + upsertedId
                   + '}';
        } else {
            return "WriteResult{acknowledged=false}";
        }
    }

    private void throwIfUnacknowledged(final String property) {
        if (!acknowledged) {
            throw new UnsupportedOperationException("Cannot get " + property + " property for an unacknowledged write");
        }
    }
}


