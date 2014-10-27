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

import com.mongodb.WriteConcernResult;
import org.bson.BsonValue;

/**
 * This class is not part of the public API.  It may change or be removed at any time.
 */
public class AcknowledgedWriteConcernResult implements WriteConcernResult {
    private final int documentsAffectedCount;
    private final boolean isUpdateOfExisting;
    private final BsonValue upsertedId;

    /**
     * Construct an instance
     *
     * @param count the count of matched documents
     * @param isUpdateOfExisting whether an existing document was updated
     * @param upsertedId if an upsert resulted in an inserted document, this is the _id of that document.  This may be null
     */
    public AcknowledgedWriteConcernResult(final int count, final boolean isUpdateOfExisting, final BsonValue upsertedId) {
        this.documentsAffectedCount = count;
        this.isUpdateOfExisting = isUpdateOfExisting;
        this.upsertedId = upsertedId;
    }

    @Override
    public boolean wasAcknowledged() {
        return true;
    }

    @Override
    public int getCount() {
        return documentsAffectedCount;
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

        AcknowledgedWriteConcernResult that = (AcknowledgedWriteConcernResult) o;

        if (documentsAffectedCount != that.documentsAffectedCount) {
            return false;
        }
        if (isUpdateOfExisting != that.isUpdateOfExisting) {
            return false;
        }
        if (upsertedId != null ? !upsertedId.equals(that.upsertedId) : that.upsertedId != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = documentsAffectedCount;
        result = 31 * result + (isUpdateOfExisting ? 1 : 0);
        result = 31 * result + (upsertedId != null ? upsertedId.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "AcknowledgedWriteResult{"
               + "documentsAffectedCount=" + documentsAffectedCount
               + ", isUpdateOfExisting=" + isUpdateOfExisting
               + ", upsertedId=" + upsertedId
               + '}';
    }
}
