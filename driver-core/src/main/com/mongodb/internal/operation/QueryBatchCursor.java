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

import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoSocketException;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.connection.ServerType;
import com.mongodb.internal.binding.ConnectionSource;
import com.mongodb.internal.connection.Connection;
import com.mongodb.internal.connection.QueryResult;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.FieldNameValidator;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Decoder;

import java.util.List;
import java.util.NoSuchElementException;

import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.operation.CursorHelper.getNumberToReturn;
import static com.mongodb.internal.operation.OperationHelper.getMoreCursorDocumentToQueryResult;
import static com.mongodb.internal.operation.QueryHelper.translateCommandException;
import static com.mongodb.internal.operation.ServerVersionHelper.serverIsAtLeastVersionThreeDotTwo;
import static java.util.Collections.singletonList;

class QueryBatchCursor<T> implements AggregateResponseBatchCursor<T> {
    private static final FieldNameValidator NO_OP_FIELD_NAME_VALIDATOR = new NoOpFieldNameValidator();
    private static final String CURSOR = "cursor";
    private static final String POST_BATCH_RESUME_TOKEN = "postBatchResumeToken";
    private static final String OPERATION_TIME = "operationTime";

    private final MongoNamespace namespace;
    private final ServerAddress serverAddress;
    private final int limit;
    private final Decoder<T> decoder;
    private final long maxTimeMS;
    private int batchSize;
    private ConnectionSource connectionSource;
    private Connection connection;
    private ServerCursor serverCursor;
    private List<T> nextBatch;
    private int count;
    private volatile boolean closed;
    private BsonDocument postBatchResumeToken;
    private BsonTimestamp operationTime;
    private final boolean firstBatchEmpty;
    private int maxWireVersion = 0;
    private boolean killCursorOnClose = true;

    QueryBatchCursor(final QueryResult<T> firstQueryResult, final int limit, final int batchSize, final Decoder<T> decoder) {
        this(firstQueryResult, limit, batchSize, decoder, null);
    }

    QueryBatchCursor(final QueryResult<T> firstQueryResult, final int limit, final int batchSize,
                     final Decoder<T> decoder, final ConnectionSource connectionSource) {
        this(firstQueryResult, limit, batchSize, 0, decoder, connectionSource, null, null);
    }

    QueryBatchCursor(final QueryResult<T> firstQueryResult, final int limit, final int batchSize, final long maxTimeMS,
                     final Decoder<T> decoder, final ConnectionSource connectionSource, final Connection connection) {
        this(firstQueryResult, limit, batchSize, maxTimeMS, decoder, connectionSource, connection, null);
    }

    QueryBatchCursor(final QueryResult<T> firstQueryResult, final int limit, final int batchSize, final long maxTimeMS,
                     final Decoder<T> decoder, final ConnectionSource connectionSource, final Connection connection,
                     final BsonDocument result) {
        isTrueArgument("maxTimeMS >= 0", maxTimeMS >= 0);
        this.maxTimeMS = maxTimeMS;
        this.namespace = firstQueryResult.getNamespace();
        this.serverAddress = firstQueryResult.getAddress();
        this.limit = limit;
        this.batchSize = batchSize;
        this.decoder = notNull("decoder", decoder);
        if (result != null) {
            this.operationTime = result.getTimestamp(OPERATION_TIME, null);
            this.postBatchResumeToken = getPostBatchResumeTokenFromResponse(result);
        }
        if (firstQueryResult.getCursor() != null) {
            notNull("connectionSource", connectionSource);
        }
        if (connectionSource != null) {
            this.connectionSource = connectionSource.retain();
        }

        initFromQueryResult(firstQueryResult);
        firstBatchEmpty = firstQueryResult.getResults().isEmpty();

        if (connection != null) {
            this.maxWireVersion = connection.getDescription().getMaxWireVersion();
            if (limitReached()) {
                killCursor(connection);
            } else {
                assertTrue(connection != null);
                if (connectionSource.getServerDescription().getType() == ServerType.LOAD_BALANCER) {
                    this.connection = connection.retain();
                    this.connection.markAsPinned(Connection.PinningMode.CURSOR);
                }
            }
        }
        releaseConnectionAndSourceIfNoServerCursor();
    }

    @Override
    public boolean hasNext() {
        if (closed) {
            throw new IllegalStateException("Cursor has been closed");
        }

        if (nextBatch != null) {
            return true;
        }

        if (limitReached()) {
            return false;
        }

        while (serverCursor != null) {
            getMore();
            if (closed) {
                throw new IllegalStateException("Cursor has been closed");
            }
            if (nextBatch != null) {
                return true;
            }
        }

        return false;
    }

    @Override
    public List<T> next() {
        if (closed) {
            throw new IllegalStateException("Iterator has been closed");
        }

        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        List<T> retVal = nextBatch;
        nextBatch = null;
        return retVal;
    }

    @Override
    public void setBatchSize(final int batchSize) {
        this.batchSize = batchSize;
    }

    @Override
    public int getBatchSize() {
        return batchSize;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            try {
                killCursor();
            } finally {
                if (connectionSource != null) {
                    connectionSource.release();
                }
                if (connection != null) {
                    connection.unmarkAsPinned(Connection.PinningMode.CURSOR);
                    connection.release();
                }
            }
        }
    }

    @Override
    public List<T> tryNext() {
        if (closed) {
            throw new IllegalStateException("Cursor has been closed");
        }

        if (!tryHasNext()) {
            return null;
        }
        return next();
    }

    boolean tryHasNext() {
        if (nextBatch != null) {
            return true;
        }

        if (limitReached()) {
            return false;
        }

        if (serverCursor != null) {
            getMore();
        }

        return nextBatch != null;
    }

    @Override
    public ServerCursor getServerCursor() {
        if (closed) {
            throw new IllegalStateException("Iterator has been closed");
        }

        return serverCursor;
    }

    @Override
    public ServerAddress getServerAddress() {
        if (closed) {
            throw new IllegalStateException("Iterator has been closed");
        }

        return serverAddress;
    }

    @Override
    public BsonDocument getPostBatchResumeToken() {
        return postBatchResumeToken;
    }

    @Override
    public BsonTimestamp getOperationTime() {
        return operationTime;
    }

    @Override
    public boolean isFirstBatchEmpty() {
        return firstBatchEmpty;
    }

    @Override
    public int getMaxWireVersion() {
        return maxWireVersion;
    }

    private void getMore() {
        Connection connection = getConnection();
        try {
            if (serverIsAtLeastVersionThreeDotTwo(connection.getDescription())) {
                try {
                    initFromCommandResult(connection.command(namespace.getDatabaseName(),
                            asGetMoreCommandDocument(),
                            NO_OP_FIELD_NAME_VALIDATOR,
                            ReadPreference.primary(),
                            CommandResultDocumentCodec.create(decoder, "nextBatch"),
                            connectionSource.getSessionContext(),
                            null /* As per spec, ServerApi elements are not included in getMore commands */));
                } catch (MongoCommandException e) {
                    throw translateCommandException(e, serverCursor);
                }
            } else {
                QueryResult<T> getMore = connection.getMore(namespace, serverCursor.getId(),
                        getNumberToReturn(limit, batchSize, count), decoder);
                initFromQueryResult(getMore);
            }
            if (limitReached()) {
                killCursor(connection);
            }
        } catch (MongoSocketException e) {
            // If connection is pinned, don't attempt to kill the cursor on close, since the connection is in a bad state
            if (this.connection != null) {
                killCursorOnClose = false;
            }
            throw e;
        } finally {
            connection.release();
            releaseConnectionAndSourceIfNoServerCursor();
        }
    }

    private Connection getConnection() {
        if (connection == null) {
            return connectionSource.getConnection();
        } else {
            return connection.retain();
        }
    }

    private BsonDocument asGetMoreCommandDocument() {
        BsonDocument document = new BsonDocument("getMore", new BsonInt64(serverCursor.getId()))
                                .append("collection", new BsonString(namespace.getCollectionName()));

        int batchSizeForGetMoreCommand = Math.abs(getNumberToReturn(limit, this.batchSize, count));
        if (batchSizeForGetMoreCommand != 0) {
            document.append("batchSize", new BsonInt32(batchSizeForGetMoreCommand));
        }
        if (maxTimeMS != 0) {
            document.append("maxTimeMS", new BsonInt64(maxTimeMS));
        }

        return document;
    }

    private void initFromQueryResult(final QueryResult<T> queryResult) {
        serverCursor = queryResult.getCursor();
        nextBatch = queryResult.getResults().isEmpty() ? null : queryResult.getResults();
        count += queryResult.getResults().size();
    }

    private void initFromCommandResult(final BsonDocument getMoreCommandResultDocument) {
        QueryResult<T> queryResult = getMoreCursorDocumentToQueryResult(getMoreCommandResultDocument.getDocument(CURSOR),
                                                                        connectionSource.getServerDescription().getAddress());
        postBatchResumeToken = getPostBatchResumeTokenFromResponse(getMoreCommandResultDocument);
        operationTime = getMoreCommandResultDocument.getTimestamp(OPERATION_TIME, null);
        initFromQueryResult(queryResult);
    }

    private boolean limitReached() {
        return Math.abs(limit) != 0 && count >= Math.abs(limit);
    }

    private void killCursor() {
        if (serverCursor != null && killCursorOnClose) {
            try {
                Connection connection = getConnection();
                try {
                    killCursor(connection);
                } finally {
                    connection.release();
                }
            } catch (MongoException e) {
                // Ignore exceptions from calling killCursor
            }
        }
    }

    private void killCursor(final Connection connection) {
        if (serverCursor != null) {
            notNull("connection", connection);
            try {
                if (serverIsAtLeastVersionThreeDotTwo(connection.getDescription())) {
                    connection.command(namespace.getDatabaseName(), asKillCursorsCommandDocument(), NO_OP_FIELD_NAME_VALIDATOR,
                            ReadPreference.primary(), new BsonDocumentCodec(), connectionSource.getSessionContext(),
                            connectionSource.getServerApi());
                } else {
                    connection.killCursor(namespace, singletonList(serverCursor.getId()));
                }
            } finally {
                serverCursor = null;
            }
        }
    }

    private void releaseConnectionAndSourceIfNoServerCursor() {
        if (serverCursor == null) {
            if (connectionSource != null) {
                connectionSource.release();
                connectionSource = null;
            }
            if (connection != null) {
                connection.release();
                connection = null;
            }
        }
    }

    private BsonDocument asKillCursorsCommandDocument() {
        return new BsonDocument("killCursors", new BsonString(namespace.getCollectionName()))
                       .append("cursors", new BsonArray(singletonList(new BsonInt64(serverCursor.getId()))));
    }

    private BsonDocument getPostBatchResumeTokenFromResponse(final BsonDocument result) {
        BsonDocument cursor = result.getDocument(CURSOR, null);
        if (cursor != null) {
            return cursor.getDocument(POST_BATCH_RESUME_TOKEN, null);
        }
        return null;
    }
}
