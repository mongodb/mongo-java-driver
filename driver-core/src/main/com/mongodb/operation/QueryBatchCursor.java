/*
 * Copyright (c) 2008-2017 MongoDB, Inc.
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

package com.mongodb.operation;

import com.mongodb.MongoCommandException;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.binding.ConnectionSource;
import com.mongodb.connection.Connection;
import com.mongodb.connection.QueryResult;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Decoder;

import java.util.List;
import java.util.NoSuchElementException;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.operation.CursorHelper.getNumberToReturn;
import static com.mongodb.operation.OperationHelper.getMoreCursorDocumentToQueryResult;
import static com.mongodb.operation.OperationHelper.serverIsAtLeastVersionThreeDotTwo;
import static com.mongodb.operation.QueryHelper.translateCommandException;
import static java.util.Collections.singletonList;

class QueryBatchCursor<T> implements BatchCursor<T> {
    private final MongoNamespace namespace;
    private final int limit;
    private final Decoder<T> decoder;
    private final ConnectionSource connectionSource;
    private final long maxTimeMS;
    private int batchSize;
    private ServerCursor serverCursor;
    private List<T> nextBatch;
    private int count;
    private boolean closed;

    QueryBatchCursor(final QueryResult<T> firstQueryResult, final int limit, final int batchSize, final Decoder<T> decoder) {
        this(firstQueryResult, limit, batchSize, decoder, (ConnectionSource) null);
    }

    QueryBatchCursor(final QueryResult<T> firstQueryResult, final int limit, final int batchSize,
                     final Decoder<T> decoder, final ConnectionSource connectionSource) {
        this(firstQueryResult, limit, batchSize, 0, decoder, connectionSource, null);
    }

    QueryBatchCursor(final QueryResult<T> firstQueryResult, final int limit, final int batchSize, final long maxTimeMS,
                     final Decoder<T> decoder, final ConnectionSource connectionSource, final Connection connection) {
        isTrueArgument("maxTimeMS >= 0", maxTimeMS >= 0);
        this.maxTimeMS = maxTimeMS;
        this.namespace = firstQueryResult.getNamespace();
        this.limit = limit;
        this.batchSize = batchSize;
        this.decoder = notNull("decoder", decoder);
        if (firstQueryResult.getCursor() != null) {
            notNull("connectionSource", connectionSource);
        }
        if (connectionSource != null) {
            this.connectionSource = connectionSource.retain();
        } else {
            this.connectionSource = null;
        }

        initFromQueryResult(firstQueryResult);
        if (limitReached()) {
            killCursor(connection);
        }
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

        return connectionSource.getServerDescription().getAddress();
    }

    private void getMore() {
        Connection connection = connectionSource.getConnection();
        try {
            if (serverIsAtLeastVersionThreeDotTwo(connection.getDescription())) {
                try {
                    initFromCommandResult(connection.command(namespace.getDatabaseName(),
                                                             asGetMoreCommandDocument(),
                                                             ReadPreference.primary(),
                                                             new NoOpFieldNameValidator(),
                                                             CommandResultDocumentCodec.create(decoder, "nextBatch")));
                } catch (MongoCommandException e) {
                    throw translateCommandException(e, serverCursor);
                }
            } else {
                initFromQueryResult(connection.getMore(namespace, serverCursor.getId(),
                                                       getNumberToReturn(limit, batchSize, count),
                                                       decoder));
            }
            if (limitReached()) {
                killCursor(connection);
            }
        } finally {
            connection.release();
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
        QueryResult<T> queryResult = getMoreCursorDocumentToQueryResult(getMoreCommandResultDocument.getDocument("cursor"),
                                                                        connectionSource.getServerDescription().getAddress());
        initFromQueryResult(queryResult);
    }

    private boolean limitReached() {
        return Math.abs(limit) != 0 && count >= Math.abs(limit);
    }

    private void killCursor() {
        if (serverCursor != null) {
            Connection connection = connectionSource.getConnection();
            try {
                killCursor(connection);
            } finally {
                connection.release();
            }
        }
    }

    private void killCursor(final Connection connection) {
        if (serverCursor != null) {
            notNull("connection", connection);
            if (serverIsAtLeastVersionThreeDotTwo(connection.getDescription())) {
                connection.command(namespace.getDatabaseName(), asKillCursorsCommandDocument(), ReadPreference.primary(),
                        new NoOpFieldNameValidator(), new BsonDocumentCodec());
            } else {
                connection.killCursor(namespace, singletonList(serverCursor.getId()));
            }
            serverCursor = null;
        }
    }

    private BsonDocument asKillCursorsCommandDocument() {
        return new BsonDocument("killCursors", new BsonString(namespace.getCollectionName()))
                       .append("cursors", new BsonArray(singletonList(new BsonInt64(serverCursor.getId()))));
    }
}
