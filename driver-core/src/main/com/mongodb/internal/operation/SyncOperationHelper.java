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

package com.mongodb.internal.operation;

import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ServerAddress;
import com.mongodb.client.model.Collation;
import com.mongodb.internal.ClientSideOperationTimeout;
import com.mongodb.internal.binding.ConnectionSource;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.binding.WriteBinding;
import com.mongodb.internal.bulk.IndexRequest;
import com.mongodb.internal.connection.Connection;
import com.mongodb.internal.connection.QueryResult;
import org.bson.BsonDocument;
import org.bson.codecs.Decoder;

import java.util.Collections;
import java.util.List;

import static com.mongodb.internal.operation.OperationHelper.cursorDocumentToQueryResult;

final class SyncOperationHelper {

    interface CallableWithConnection<T> {
        T call(ClientSideOperationTimeout clientSideOperationTimeout, Connection connection);
    }

    interface CallableWithSource<T> {
        T call(ClientSideOperationTimeout clientSideOperationTimeout, ConnectionSource source);
    }

    interface CallableWithConnectionAndSource<T> {
        T call(ClientSideOperationTimeout clientSideOperationTimeout, ConnectionSource source, Connection connection);
    }

    static void validateReadConcern(final Connection connection, final ReadConcern readConcern) {
        OperationHelper.validateReadConcern(connection.getDescription(), readConcern);
    }

    static void validateCollation(final Connection connection, final Collation collation) {
        OperationHelper.validateCollation(connection.getDescription(), collation);
    }

    static void validateAllowDiskUse(final Connection connection, final Boolean allowDiskUse) {
        OperationHelper.validateAllowDiskUse(connection.getDescription(), allowDiskUse).ifPresent(throwable -> {
            throw new IllegalArgumentException(throwable.getMessage());
        });
    }

    static void validateIndexRequestCollations(final Connection connection, final List<IndexRequest> requests) {
        for (IndexRequest request : requests) {
            if (request.getCollation() != null) {
                validateCollation(connection, request.getCollation());
                break;
            }
        }
    }

    static void validateFindOptions(final Connection connection, final ReadConcern readConcern, final Collation collation,
                                    final Boolean allowDiskUse) {
        validateReadConcernAndCollation(connection, readConcern, collation);
        validateAllowDiskUse(connection, allowDiskUse);
    }

    static void validateReadConcernAndCollation(final Connection connection, final ReadConcern readConcern,
                                                final Collation collation) {
        validateReadConcern(connection, readConcern);
        validateCollation(connection, collation);
    }

    static <T> QueryBatchCursor<T> createEmptyBatchCursor(final ClientSideOperationTimeout clientSideOperationTimeout,
                                                          final MongoNamespace namespace, final Decoder<T> decoder,
                                                          final ServerAddress serverAddress, final int batchSize) {
        return new QueryBatchCursor<>(clientSideOperationTimeout, new QueryResult<>(namespace, Collections.emptyList(), 0L,
                serverAddress), 0, batchSize, decoder);
    }

    static <T> BatchCursor<T> cursorDocumentToBatchCursor(final ClientSideOperationTimeout clientSideOperationTimeout,
                                                          final BsonDocument cursorDocument, final Decoder<T> decoder,
                                                          final ConnectionSource source, final int batchSize) {
        return new QueryBatchCursor<>(clientSideOperationTimeout, cursorDocumentToQueryResult(cursorDocument,
                source.getServerDescription().getAddress()), 0, batchSize, decoder, source);
    }

    static <T> T withConnection(final ClientSideOperationTimeout clientSideOperationTimeout, final ReadBinding binding,
                                final CallableWithConnection<T> callable) {
        ConnectionSource source = binding.getReadConnectionSource();
        try {
            return withConnectionSource(clientSideOperationTimeout, source, callable);
        } finally {
            source.release();
        }
    }

    static <T> T withConnection(final ClientSideOperationTimeout clientSideOperationTimeout, final ReadBinding binding,
                                final CallableWithConnectionAndSource<T> callable) {
        ConnectionSource source = binding.getReadConnectionSource();
        try {
            return withConnectionSource(clientSideOperationTimeout, source, callable);
        } finally {
            source.release();
        }
    }

    static <T> T withReadConnectionSource(final ClientSideOperationTimeout clientSideOperationTimeout, final ReadBinding binding,
                                          final CallableWithSource<T> callable) {
        ConnectionSource source = binding.getReadConnectionSource();
        try {
            return callable.call(clientSideOperationTimeout, source);
        } finally {
            source.release();
        }
    }

    static <T> T withReleasableConnection(final ClientSideOperationTimeout clientSideOperationTimeout, final ReadBinding binding,
                                          final MongoException connectionException, final CallableWithConnectionAndSource<T> callable) {
        ConnectionSource source = null;
        Connection connection;
        try {
            source = binding.getReadConnectionSource();
            connection = source.getConnection();
        } catch (Throwable t){
            if (source != null) {
                source.release();
            }
            throw connectionException;
        }
        try {
            return callable.call(clientSideOperationTimeout, source, connection);
        } finally {
            source.release();
        }
    }

    static <T> T withConnection(final ClientSideOperationTimeout clientSideOperationTimeout, final WriteBinding binding,
                                final CallableWithConnectionAndSource<T> callable) {
        ConnectionSource source = binding.getWriteConnectionSource();
        try {
            return withConnectionSource(clientSideOperationTimeout, source, callable);
        } finally {
            source.release();
        }
    }

    static <T> T withConnection(final ClientSideOperationTimeout clientSideOperationTimeout, final WriteBinding binding,
                                final CallableWithConnection<T> callable) {
        ConnectionSource source = binding.getWriteConnectionSource();
        try {
            return withConnectionSource(clientSideOperationTimeout, source, callable);
        } finally {
            source.release();
        }
    }

    static <T> T withReleasableConnection(final ClientSideOperationTimeout clientSideOperationTimeout, final WriteBinding binding,
                                          final CallableWithConnectionAndSource<T> callable) {
        ConnectionSource source = binding.getWriteConnectionSource();
        try {
            return callable.call(clientSideOperationTimeout, source, source.getConnection());
        } finally {
            source.release();
        }
    }

    static <T> T withReleasableConnection(final ClientSideOperationTimeout clientSideOperationTimeout, final WriteBinding binding,
                                          final MongoException connectionException, final CallableWithConnectionAndSource<T> callable) {
        ConnectionSource source = null;
        Connection connection;
        try {
            source = binding.getWriteConnectionSource();
            connection = source.getConnection();
        } catch (Throwable t){
            if (source != null) {
                source.release();
            }
            throw connectionException;
        }
        try {
            return callable.call(clientSideOperationTimeout, source, connection);
        } finally {
            source.release();
        }
    }

    static <T> T withConnectionSource(final ClientSideOperationTimeout clientSideOperationTimeout, final ConnectionSource source,
                                      final CallableWithConnection<T> callable) {
        Connection connection = source.getConnection();
        try {
            return callable.call(clientSideOperationTimeout, connection);
        } finally {
            connection.release();
        }
    }

    static <T> T withConnectionSource(final ClientSideOperationTimeout clientSideOperationTimeout, final ConnectionSource source,
                                      final CallableWithConnectionAndSource<T> callable) {
        Connection connection = source.getConnection();
        try {
            return callable.call(clientSideOperationTimeout, source, connection);
        } finally {
            connection.release();
        }
    }

    private SyncOperationHelper(){
    }

}
