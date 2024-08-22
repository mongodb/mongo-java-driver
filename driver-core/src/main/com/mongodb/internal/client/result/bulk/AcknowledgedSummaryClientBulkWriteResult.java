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

import java.util.Objects;
import java.util.Optional;

import static java.util.Optional.empty;

/**
 * This class is not part of the public API and may be removed or changed at any time.
 */
public final class AcknowledgedSummaryClientBulkWriteResult implements ClientBulkWriteResult {
    private final long insertedCount;
    private final long upsertedCount;
    private final long matchedCount;
    private final long modifiedCount;
    private final long deletedCount;

    public AcknowledgedSummaryClientBulkWriteResult(
            final long insertedCount,
            final long upsertedCount,
            final long matchedCount,
            final long modifiedCount,
            final long deletedCount) {
        this.insertedCount = insertedCount;
        this.upsertedCount = upsertedCount;
        this.matchedCount = matchedCount;
        this.modifiedCount = modifiedCount;
        this.deletedCount = deletedCount;
    }

    @Override
    public boolean isAcknowledged() {
        return true;
    }

    @Override
    public long getInsertedCount() {
        return insertedCount;
    }

    @Override
    public long getUpsertedCount() {
        return upsertedCount;
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
    public long getDeletedCount() {
        return deletedCount;
    }

    @Override
    public Optional<Verbose> getVerbose() {
        return empty();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final AcknowledgedSummaryClientBulkWriteResult that = (AcknowledgedSummaryClientBulkWriteResult) o;
        return insertedCount == that.insertedCount
                && upsertedCount == that.upsertedCount
                && matchedCount == that.matchedCount
                && modifiedCount == that.modifiedCount
                && deletedCount == that.deletedCount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(insertedCount, upsertedCount, matchedCount, modifiedCount, deletedCount);
    }

    @Override
    public String toString() {
        return "AcknowledgedSummaryClientBulkWriteResult{"
                + "insertedCount=" + insertedCount
                + ", upsertedCount=" + upsertedCount
                + ", matchedCount=" + matchedCount
                + ", modifiedCount=" + modifiedCount
                + ", deletedCount=" + deletedCount
                + '}';
    }
}
