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

import com.mongodb.bulk.WriteConcernError;
import com.mongodb.client.model.bulk.ClientWriteModel;
import com.mongodb.client.result.bulk.ClientBulkWriteResult;
import com.mongodb.lang.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Optional.ofNullable;

/**
 * The result of an unsuccessful or partially unsuccessful client-level bulk write operation.
 *
 * @see ClientBulkWriteResult
 * @since 5.3
 * @serial exclude
 */
public final class ClientBulkWriteException extends MongoServerException {
    private static final long serialVersionUID = 1;

    @Nullable
    private final MongoException error;
    private final List<WriteConcernError> writeConcernErrors;
    private final Map<Integer, WriteError> writeErrors;
    @Nullable
    private final ClientBulkWriteResult partialResult;

    /**
     * Constructs a new instance.
     *
     * @param error The {@linkplain #getError() top-level error}.
     * @param writeConcernErrors The {@linkplain #getWriteConcernErrors() write concern errors}.
     * @param writeErrors The {@linkplain #getWriteErrors() write errors}.
     * @param partialResult The {@linkplain #getPartialResult() partial result}.
     * @param serverAddress The {@linkplain MongoServerException#getServerAddress() server address}.
     */
    public ClientBulkWriteException(
            @Nullable final MongoException error,
            @Nullable final List<WriteConcernError> writeConcernErrors,
            @Nullable final Map<Integer, WriteError> writeErrors,
            @Nullable final ClientBulkWriteResult partialResult,
            final ServerAddress serverAddress) {
        super(message(error, writeConcernErrors, writeErrors, partialResult, serverAddress), serverAddress);
        // BULK-TODO Should ClientBulkWriteException.getCode be the same as error.getCode,
        // and getErrorLabels/hasErrorLabel contain the same labels as error.getErrorLabels?
        isTrueArgument("At least one of `writeConcernErrors`, `writeErrors`, `partialResult` must be non-null or non-empty",
                !(writeConcernErrors == null || writeConcernErrors.isEmpty())
                        || !(writeErrors == null || writeErrors.isEmpty())
                        || partialResult != null);
        this.error = error;
        this.writeConcernErrors = writeConcernErrors == null ? emptyList() : unmodifiableList(writeConcernErrors);
        this.writeErrors = writeErrors == null ? emptyMap() : unmodifiableMap(writeErrors);
        this.partialResult = partialResult;
    }

    private static String message(
            @Nullable final MongoException error,
            @Nullable final List<WriteConcernError> writeConcernErrors,
            @Nullable final Map<Integer, WriteError> writeErrors,
            @Nullable final ClientBulkWriteResult partialResult,
            final ServerAddress serverAddress) {
        return "Client-level bulk write operation error on server " + serverAddress + "."
                + (error == null ? "" : " Top-level error: " + error + ".")
                + (writeErrors == null || writeErrors.isEmpty() ? "" : " Write errors: " + writeErrors + ".")
                + (writeConcernErrors == null || writeConcernErrors.isEmpty() ? "" : " Write concern errors: " + writeConcernErrors + ".")
                + (partialResult == null ? "" : " Partial result: " + partialResult + ".");
    }

    /**
     * The top-level error. That is an error that is neither a {@linkplain #getWriteConcernErrors() write concern error},
     * nor is an {@linkplain #getWriteErrors() error of an individual write operation}.
     *
     * @return The top-level error. {@linkplain Optional#isPresent() Present} only if a top-level error occurred.
     */
    public Optional<MongoException> getError() {
        return ofNullable(error);
    }

    /**
     * The {@link WriteConcernError}s that occurred while executing the client-level bulk write operation.
     * <p>
     * There are no guarantees on mutability of the {@link List} returned.</p>
     *
     * @return The {@link WriteConcernError}s.
     */
    public List<WriteConcernError> getWriteConcernErrors() {
        return writeConcernErrors;
    }

    /**
     * The indexed {@link WriteError}s.
     * The {@linkplain Map#keySet() keys} are the indexes of the corresponding {@link ClientWriteModel}s
     * in the corresponding client-level bulk write operation.
     * <p>
     * There are no guarantees on mutability or iteration order of the {@link Map} returned.</p>
     *
     * @return The indexed {@link WriteError}s.
     * @see ClientBulkWriteResult.Verbose#getInsertResults()
     * @see ClientBulkWriteResult.Verbose#getUpdateResults()
     * @see ClientBulkWriteResult.Verbose#getDeleteResults()
     */
    public Map<Integer, WriteError> getWriteErrors() {
        return writeErrors;
    }

    /**
     * The result of the successful part of a client-level bulk write operation.
     *
     * @return The successful partial result. {@linkplain Optional#isPresent() Present} only if at least one
     * {@linkplain ClientWriteModel individual write operation} succeed.
     */
    public Optional<ClientBulkWriteResult> getPartialResult() {
        return ofNullable(partialResult);
    }
}
