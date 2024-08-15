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
package com.mongodb.internal.client.result.bulk;

import com.mongodb.client.result.bulk.ClientBulkWriteResult;
import com.mongodb.client.result.bulk.ClientDeleteResult;
import com.mongodb.client.result.bulk.ClientInsertOneResult;
import com.mongodb.client.result.bulk.ClientUpdateResult;

import java.util.Map;
import java.util.Objects;

import static java.util.Collections.unmodifiableMap;

/**
 * This class is not part of the public API and may be removed or changed at any time.
 */
public final class AcknowledgedVerboseClientBulkWriteResult implements ClientBulkWriteResult {
    private final AcknowledgedSummaryClientBulkWriteResult summaryResults;
    private final Map<Long, ClientInsertOneResult> insertResults;
    private final Map<Long, ClientUpdateResult> updateResults;
    private final Map<Long, ClientDeleteResult> deleteResults;

    public AcknowledgedVerboseClientBulkWriteResult(
            final AcknowledgedSummaryClientBulkWriteResult summaryResults,
            final Map<Long, ClientInsertOneResult> insertResults,
            final Map<Long, ClientUpdateResult> updateResults,
            final Map<Long, ClientDeleteResult> deleteResults) {
        this.summaryResults = summaryResults;
        this.insertResults = unmodifiableMap(insertResults);
        this.updateResults = unmodifiableMap(updateResults);
        this.deleteResults = unmodifiableMap(deleteResults);
    }

    @Override
    public boolean isAcknowledged() {
        return true;
    }

    @Override
    public boolean hasVerboseResults() {
        return true;
    }

    @Override
    public long getInsertedCount() {
        return summaryResults.getInsertedCount();
    }

    @Override
    public long getUpsertedCount() {
        return summaryResults.getUpsertedCount();
    }

    @Override
    public long getMatchedCount() {
        return summaryResults.getMatchedCount();
    }

    @Override
    public long getModifiedCount() {
        return summaryResults.getModifiedCount();
    }

    @Override
    public long getDeletedCount() {
        return summaryResults.getDeletedCount();
    }

    @Override
    public Map<Long, ClientInsertOneResult> getInsertResults() {
        return insertResults;
    }

    @Override
    public Map<Long, ClientUpdateResult> getUpdateResults() {
        return updateResults;
    }

    @Override
    public Map<Long, ClientDeleteResult> getDeleteResults() {
        return deleteResults;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final AcknowledgedVerboseClientBulkWriteResult that = (AcknowledgedVerboseClientBulkWriteResult) o;
        return Objects.equals(summaryResults, that.summaryResults)
                && Objects.equals(insertResults, that.insertResults)
                && Objects.equals(updateResults, that.updateResults)
                && Objects.equals(deleteResults, that.deleteResults);
    }

    @Override
    public int hashCode() {
        return Objects.hash(summaryResults, insertResults, updateResults, deleteResults);
    }

    @Override
    public String toString() {
        return "AcknowledgedVerboseClientBulkWriteResult{"
                + "insertedCount=" + summaryResults.getInsertedCount()
                + ", upsertedCount=" + summaryResults.getUpsertedCount()
                + ", matchedCount=" + summaryResults.getMatchedCount()
                + ", modifiedCount=" + summaryResults.getModifiedCount()
                + ", deletedCount=" + summaryResults.getDeletedCount()
                + ", insertResults=" + insertResults
                + ", updateResults=" + updateResults
                + ", deleteResults=" + deleteResults
                + '}';
    }
}
