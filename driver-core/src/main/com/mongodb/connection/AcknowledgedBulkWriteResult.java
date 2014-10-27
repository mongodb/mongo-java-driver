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

package com.mongodb.connection;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.BulkWriteUpsert;
import com.mongodb.bulk.WriteRequest;

import java.util.Collections;
import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * This class is not part of the public API.
 */
public class AcknowledgedBulkWriteResult extends BulkWriteResult {
    private int insertedCount;
    private int matchedCount;
    private int removedCount;
    private Integer modifiedCount;
    private final List<BulkWriteUpsert> upserts;

    /**
     * Construct an instance.
     *
     * @param type the type of the write
     * @param count the number of documents matched
     * @param upserts the list of upserts
     */
    public AcknowledgedBulkWriteResult(final WriteRequest.Type type, final int count, final List<BulkWriteUpsert> upserts) {
        this(type, count, 0, upserts);
    }

    /**
     * Construct an instance.
     *
     * @param type the type of the write
     * @param count the number of documents matched
     * @param modifiedCount the number of documents modified, which may be null if the server was not able to provide the count
     * @param upserts the list of upserts
     */
    public AcknowledgedBulkWriteResult(final WriteRequest.Type type, final int count, final Integer modifiedCount,
                                       final List<BulkWriteUpsert> upserts) {
        this(type == WriteRequest.Type.INSERT ? count : 0,
             (type == WriteRequest.Type.UPDATE || type == WriteRequest.Type.REPLACE)  ? count : 0,
             type == WriteRequest.Type.DELETE ? count : 0,
             modifiedCount, upserts);
    }

    AcknowledgedBulkWriteResult(final int insertedCount, final int matchedCount, final int removedCount,
                                final Integer modifiedCount, final List<BulkWriteUpsert> upserts) {
        this.insertedCount = insertedCount;
        this.matchedCount = matchedCount;
        this.removedCount = removedCount;
        this.modifiedCount = modifiedCount;
        this.upserts = Collections.unmodifiableList(notNull("upserts", upserts));
    }

    @Override
    public boolean isAcknowledged() {
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
    public int getRemovedCount() {
        return removedCount;
    }

    @Override
    public boolean isModifiedCountAvailable() {
        return modifiedCount != null;
    }

    @Override
    public int getModifiedCount() {
        if (modifiedCount == null) {
            throw new UnsupportedOperationException("The modifiedCount is not available because at least one of the servers that was "
                                                    + "updated was not able to provide this information (the server is must be at least "
                                                    + "version 2.6");
        }
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

        AcknowledgedBulkWriteResult that = (AcknowledgedBulkWriteResult) o;

        if (insertedCount != that.insertedCount) {
            return false;
        }
        if (modifiedCount != null ? !modifiedCount.equals(that.modifiedCount) : that.modifiedCount != null) {
            return false;
        }
        if (removedCount != that.removedCount) {
            return false;
        }
        if (matchedCount != that.matchedCount) {
            return false;
        }
        if (!upserts.equals(that.upserts)) {
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
}
