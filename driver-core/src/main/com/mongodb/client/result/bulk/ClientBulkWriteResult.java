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
package com.mongodb.client.result.bulk;

import com.mongodb.ClientBulkWriteException;
import com.mongodb.WriteConcern;
import com.mongodb.annotations.Evolving;
import com.mongodb.bulk.WriteConcernError;
import com.mongodb.client.model.bulk.ClientBulkWriteOptions;
import com.mongodb.client.model.bulk.ClientWriteModel;

import java.util.Map;

/**
 * The result of a successful or partially successful client-level bulk write operation.
 * Note that if only some of the {@linkplain ClientWriteModel individual write operations} succeed,
 * or if there are {@link WriteConcernError}s, then the successful partial result
 * is still accessible via {@link ClientBulkWriteException#getPartialResult()}.
 *
 * @see ClientBulkWriteException
 * @since 5.3
 */
@Evolving
public interface ClientBulkWriteResult {
    /**
     * Indicated whether this result was {@linkplain WriteConcern#isAcknowledged() acknowledged}.
     * If not, then all other methods throw {@link UnsupportedOperationException}.
     *
     * @return Whether this result was acknowledged.
     */
    boolean isAcknowledged();

    /**
     * Indicates whether there are {@linkplain ClientBulkWriteOptions#verboseResults(Boolean) verbose results}
     * of individual operations.
     * If not, then {@link #getInsertResults()}, {@link #getUpdateResults()}, {@link #getDeleteResults()}
     * throw {@link UnsupportedOperationException}.
     *
     * @return Whether there are verbose results.
     * @throws UnsupportedOperationException If this result is not {@linkplain #isAcknowledged() acknowledged}.
     */
    // BULK-TODO Do we still have getInsertedCount etc., when there are no verbose results?
    boolean hasVerboseResults() throws UnsupportedOperationException;

    /**
     * The number of documents that were inserted across all insert operations.
     *
     * @return The number of documents that were inserted.
     * @throws UnsupportedOperationException If this result is not {@linkplain #isAcknowledged() acknowledged}.
     */
    long getInsertedCount() throws UnsupportedOperationException;

    /**
     * The number of documents that were upserted across all update and replace operations.
     *
     * @return The number of documents that were upserted.
     * @throws UnsupportedOperationException If this result is not {@linkplain #isAcknowledged() acknowledged}.
     */
    long getUpsertedCount() throws UnsupportedOperationException;

    /**
     * The number of documents that matched the filters across all operations with filters.
     *
     * @return The number of documents that were matched.
     * @throws UnsupportedOperationException If this result is not {@linkplain #isAcknowledged() acknowledged}.
     */
    long getMatchedCount() throws UnsupportedOperationException;

    /**
     * The number of documents that were modified across all update and replace operations.
     *
     * @return The number of documents that were modified.
     * @throws UnsupportedOperationException If this result is not {@linkplain #isAcknowledged() acknowledged}.
     */
    long getModifiedCount() throws UnsupportedOperationException;

    // BULK-TODO Is ReplaceOne reported as 1 modified (I expect this behavior), or 1 deleted and 1 inserted?

    /**
     * The number of documents that were deleted across all delete operations.
     *
     * @return The number of documents that were deleted.
     * @throws UnsupportedOperationException If this result is not {@linkplain #isAcknowledged() acknowledged}.
     */
    long getDeletedCount() throws UnsupportedOperationException;

    /**
     * The indexed {@link ClientInsertOneResult}s.
     * The {@linkplain Map#keySet() keys} are the indexes of the corresponding {@link ClientWriteModel}s
     * in the client-level bulk write operation.
     * <p>
     * There are no guarantees on mutability or iteration order of the {@link Map} returned.</p>
     *
     * @return The indexed {@link ClientInsertOneResult}s.
     * @throws UnsupportedOperationException If this result is either not {@linkplain #isAcknowledged() acknowledged},
     * or does not have {@linkplain #hasVerboseResults() verbose results}.
     * @see ClientBulkWriteException#getWriteErrors()
     */
    Map<Long, ClientInsertOneResult> getInsertResults() throws UnsupportedOperationException;

    /**
     * The indexed {@link ClientUpdateResult}s.
     * The {@linkplain Map#keySet() keys} are the indexes of the corresponding {@link ClientWriteModel}s
     * in the client-level bulk write operation.
     * <p>
     * There are no guarantees on mutability or iteration order of the {@link Map} returned.</p>
     *
     * @return The indexed {@link ClientUpdateResult}s.
     * @throws UnsupportedOperationException If this result is either not {@linkplain #isAcknowledged() acknowledged},
     * or does not have {@linkplain #hasVerboseResults() verbose results}.
     * @see ClientBulkWriteException#getWriteErrors()
     */
    Map<Long, ClientUpdateResult> getUpdateResults() throws UnsupportedOperationException;

    /**
     * The indexed {@link ClientDeleteResult}s.
     * The {@linkplain Map#keySet() keys} are the indexes of the corresponding {@link ClientWriteModel}s
     * in the client-level bulk write operation.
     * <p>
     * There are no guarantees on mutability or iteration order of the {@link Map} returned.</p>
     *
     * @return The indexed {@link ClientDeleteResult}s.
     * @throws UnsupportedOperationException If this result is either not {@linkplain #isAcknowledged() acknowledged},
     * or does not have {@linkplain #hasVerboseResults() verbose results}.
     * @see ClientBulkWriteException#getWriteErrors()
     */
    Map<Long, ClientDeleteResult> getDeleteResults() throws UnsupportedOperationException;
}
