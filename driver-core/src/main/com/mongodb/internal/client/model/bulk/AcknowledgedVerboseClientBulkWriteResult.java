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
package com.mongodb.internal.client.model.bulk;

import com.mongodb.client.model.bulk.ClientBulkWriteResult;
import com.mongodb.client.model.bulk.ClientDeleteResult;
import com.mongodb.client.model.bulk.ClientInsertOneResult;
import com.mongodb.client.model.bulk.ClientUpdateResult;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static java.util.Optional.of;

/**
 * This class is not part of the public API and may be removed or changed at any time.
 */
public final class AcknowledgedVerboseClientBulkWriteResult implements ClientBulkWriteResult {
    private final AcknowledgedSummaryClientBulkWriteResult summaryResults;
    private final AcknowledgedVerboseClientBulkWriteResult.Verbose verbose;

    public AcknowledgedVerboseClientBulkWriteResult(
            final AcknowledgedSummaryClientBulkWriteResult summaryResults,
            final Map<Integer, ClientInsertOneResult> insertResults,
            final Map<Integer, ClientUpdateResult> updateResults,
            final Map<Integer, ClientDeleteResult> deleteResults) {
        this.summaryResults = summaryResults;
        this.verbose = new AcknowledgedVerboseClientBulkWriteResult.Verbose(insertResults, updateResults, deleteResults);
    }

    @Override
    public boolean isAcknowledged() {
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
    public Optional<ClientBulkWriteResult.Verbose> getVerbose() {
        return of(verbose);
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
                && Objects.equals(verbose, that.verbose);
    }

    @Override
    public int hashCode() {
        return Objects.hash(summaryResults, verbose);
    }

    @Override
    public String toString() {
        return "AcknowledgedVerboseClientBulkWriteResult{"
                + "insertedCount=" + summaryResults.getInsertedCount()
                + ", upsertedCount=" + summaryResults.getUpsertedCount()
                + ", matchedCount=" + summaryResults.getMatchedCount()
                + ", modifiedCount=" + summaryResults.getModifiedCount()
                + ", deletedCount=" + summaryResults.getDeletedCount()
                + ", insertResults=" + verbose.insertResults
                + ", updateResults=" + verbose.updateResults
                + ", deleteResults=" + verbose.deleteResults
                + '}';
    }

    private static final class Verbose implements ClientBulkWriteResult.Verbose {
        private final Map<Integer, ClientInsertOneResult> insertResults;
        private final Map<Integer, ClientUpdateResult> updateResults;
        private final Map<Integer, ClientDeleteResult> deleteResults;

        Verbose(
                final Map<Integer, ClientInsertOneResult> insertResults,
                final Map<Integer, ClientUpdateResult> updateResults,
                final Map<Integer, ClientDeleteResult> deleteResults) {
            this.insertResults = insertResults;
            this.updateResults = updateResults;
            this.deleteResults = deleteResults;
        }

        @Override
        public Map<Integer, ClientInsertOneResult> getInsertResults() {
            return insertResults;
        }

        @Override
        public Map<Integer, ClientUpdateResult> getUpdateResults() {
            return updateResults;
        }

        @Override
        public Map<Integer, ClientDeleteResult> getDeleteResults() {
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
            final AcknowledgedVerboseClientBulkWriteResult.Verbose verbose = (AcknowledgedVerboseClientBulkWriteResult.Verbose) o;
            return Objects.equals(insertResults, verbose.insertResults)
                    && Objects.equals(updateResults, verbose.updateResults)
                    && Objects.equals(deleteResults, verbose.deleteResults);
        }

        @Override
        public int hashCode() {
            return Objects.hash(insertResults, updateResults, deleteResults);
        }

        @Override
        public String toString() {
            return "AcknowledgedVerboseClientBulkWriteResult.Verbose{"
                    + "insertResults=" + insertResults
                    + ", updateResults=" + updateResults
                    + ", deleteResults=" + deleteResults
                    + '}';
        }
    }
}
