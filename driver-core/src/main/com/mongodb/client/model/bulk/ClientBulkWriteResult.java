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
package com.mongodb.client.model.bulk;

import com.mongodb.ClientBulkWriteException;
import com.mongodb.WriteConcern;
import com.mongodb.annotations.Evolving;
import com.mongodb.bulk.WriteConcernError;

import java.util.Map;
import java.util.Optional;

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
     * The number of documents that were inserted across all insert operations.
     *
     * @return The number of documents that were inserted.
     * @throws UnsupportedOperationException If this result is not {@linkplain #isAcknowledged() acknowledged}.
     */
    long getInsertedCount();

    /**
     * The number of documents that were upserted across all update and replace operations.
     *
     * @return The number of documents that were upserted.
     * @throws UnsupportedOperationException If this result is not {@linkplain #isAcknowledged() acknowledged}.
     */
    long getUpsertedCount();

    /**
     * The number of documents that matched the filters across all operations with filters.
     *
     * @return The number of documents that were matched.
     * @throws UnsupportedOperationException If this result is not {@linkplain #isAcknowledged() acknowledged}.
     */
    long getMatchedCount();

    /**
     * The number of documents that were modified across all update and replace operations.
     *
     * @return The number of documents that were modified.
     * @throws UnsupportedOperationException If this result is not {@linkplain #isAcknowledged() acknowledged}.
     */
    long getModifiedCount();

    /**
     * The number of documents that were deleted across all delete operations.
     *
     * @return The number of documents that were deleted.
     * @throws UnsupportedOperationException If this result is not {@linkplain #isAcknowledged() acknowledged}.
     */
    long getDeletedCount();

    /**
     * The verbose results of individual operations.
     *
     * @return {@link Optional} verbose results of individual operations.
     * @throws UnsupportedOperationException If this result is not {@linkplain #isAcknowledged() acknowledged}.
     * @see ClientBulkWriteOptions#verboseResults(Boolean)
     */
    Optional<Verbose> getVerbose();

    /**
     * The {@linkplain ClientBulkWriteResult#getVerbose() verbose results} of individual operations.
     *
     * @since 5.3
     */
    @Evolving
    interface Verbose {
        /**
         * The indexed {@link ClientInsertOneResult}s.
         * The {@linkplain Map#keySet() keys} are the indexes of the corresponding {@link ClientWriteModel}s
         * in the client-level bulk write operation.
         * <p>
         * There are no guarantees on mutability or iteration order of the {@link Map} returned.</p>
         *
         * @return The indexed {@link ClientInsertOneResult}s.
         * @see ClientBulkWriteException#getWriteErrors()
         */
        Map<Integer, ClientInsertOneResult> getInsertResults();

        /**
         * The indexed {@link ClientUpdateResult}s.
         * The {@linkplain Map#keySet() keys} are the indexes of the corresponding {@link ClientWriteModel}s
         * in the client-level bulk write operation.
         * <p>
         * There are no guarantees on mutability or iteration order of the {@link Map} returned.</p>
         *
         * @return The indexed {@link ClientUpdateResult}s.
         * @see ClientBulkWriteException#getWriteErrors()
         */
        Map<Integer, ClientUpdateResult> getUpdateResults();

        /**
         * The indexed {@link ClientDeleteResult}s.
         * The {@linkplain Map#keySet() keys} are the indexes of the corresponding {@link ClientWriteModel}s
         * in the client-level bulk write operation.
         * <p>
         * There are no guarantees on mutability or iteration order of the {@link Map} returned.</p>
         *
         * @return The indexed {@link ClientDeleteResult}s.
         * @see ClientBulkWriteException#getWriteErrors()
         */
        Map<Integer, ClientDeleteResult> getDeleteResults();
    }
}
