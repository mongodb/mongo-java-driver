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

import com.mongodb.annotations.Immutable;
import com.mongodb.client.model.bulk.ClientBulkWriteResult;

import java.util.Optional;

/**
 * This class is not part of the public API and may be removed or changed at any time.
 */
@Immutable
public final class UnacknowledgedClientBulkWriteResult implements ClientBulkWriteResult {
    public static final UnacknowledgedClientBulkWriteResult INSTANCE = new UnacknowledgedClientBulkWriteResult();

    private UnacknowledgedClientBulkWriteResult() {
    }

    @Override
    public boolean isAcknowledged() {
        return false;
    }

    @Override
    public long getInsertedCount() throws UnsupportedOperationException {
        throw createUnacknowledgedResultsException();
    }

    @Override
    public long getUpsertedCount() throws UnsupportedOperationException {
        throw createUnacknowledgedResultsException();
    }

    @Override
    public long getMatchedCount() throws UnsupportedOperationException {
        throw createUnacknowledgedResultsException();
    }

    @Override
    public long getModifiedCount() throws UnsupportedOperationException {
        throw createUnacknowledgedResultsException();
    }

    @Override
    public long getDeletedCount() throws UnsupportedOperationException {
        throw createUnacknowledgedResultsException();
    }

    @Override
    public Optional<Verbose> getVerbose() throws UnsupportedOperationException {
        throw createUnacknowledgedResultsException();
    }

    private static UnsupportedOperationException createUnacknowledgedResultsException() {
        return new UnsupportedOperationException("Cannot get information about an unacknowledged write");
    }

    @Override
    public String toString() {
        return "UnacknowledgedClientBulkWriteResult{}";
    }
}
