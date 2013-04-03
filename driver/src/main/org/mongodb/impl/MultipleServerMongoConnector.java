/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb.impl;

import org.mongodb.Document;
import org.mongodb.MongoClientOptions;
import org.mongodb.MongoConnectionStrategy;
import org.mongodb.MongoConnector;
import org.mongodb.MongoNamespace;
import org.mongodb.ReadPreference;
import org.mongodb.ServerAddress;
import org.mongodb.async.SingleResultCallback;
import org.mongodb.io.BufferPool;
import org.mongodb.io.PowerOfTwoByteBufferPool;
import org.mongodb.operation.GetMore;
import org.mongodb.operation.MongoCommand;
import org.mongodb.operation.MongoFind;
import org.mongodb.operation.MongoInsert;
import org.mongodb.operation.MongoKillCursor;
import org.mongodb.operation.MongoRemove;
import org.mongodb.operation.MongoReplace;
import org.mongodb.operation.MongoUpdate;
import org.mongodb.result.CommandResult;
import org.mongodb.result.QueryResult;
import org.mongodb.result.ServerCursor;
import org.mongodb.result.WriteResult;
import org.mongodb.Codec;
import org.mongodb.Decoder;
import org.mongodb.Encoder;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

class MultipleServerMongoConnector implements MongoConnector {
    private final MongoClientOptions options;
    private final BufferPool<ByteBuffer> bufferPool;
    private final Map<ServerAddress, SingleServerMongoConnector> mongoClientMap =
            new HashMap<ServerAddress, SingleServerMongoConnector>();
    private final MongoConnectionStrategy connectionStrategy;

    MultipleServerMongoConnector(final MongoConnectionStrategy connectionStrategy, final MongoClientOptions options) {
        this.connectionStrategy = connectionStrategy;
        this.options = options;
        this.bufferPool = new PowerOfTwoByteBufferPool();
    }

    @Override
    public CommandResult command(final String database, final MongoCommand commandOperation,
                                 final Codec<Document> codec) {
        return getConnection(commandOperation.getReadPreference()).command(database, commandOperation, codec);
    }

    @Override
    public <T> QueryResult<T> query(final MongoNamespace namespace, final MongoFind find,
                                    final Encoder<Document> queryEncoder, final Decoder<T> resultDecoder) {
        return getConnection(find.getReadPreference()).query(namespace, find, queryEncoder, resultDecoder);
    }

    @Override
    public <T> QueryResult<T> getMore(final MongoNamespace namespace, final GetMore getMore,
                                      final Decoder<T> resultDecoder) {
        return getConnection(getMore.getServerCursor().getAddress()).getMore(namespace, getMore, resultDecoder);
    }

    @Override
    public void killCursors(final MongoKillCursor killCursor) {
        for (ServerCursor cursor : killCursor.getServerCursors()) {
            getConnection(cursor.getAddress()).killCursors(new MongoKillCursor(cursor));
        }
    }

    @Override
    public <T> WriteResult insert(final MongoNamespace namespace, final MongoInsert<T> insert, final Encoder<T> encoder) {
        return getPrimary().insert(namespace, insert, encoder);
    }

    @Override
    public WriteResult update(final MongoNamespace namespace, final MongoUpdate update, final Encoder<Document> queryEncoder) {
        return getPrimary().update(namespace, update, queryEncoder);
    }

    @Override
    public <T> WriteResult replace(final MongoNamespace namespace, final MongoReplace<T> replace,
                                   final Encoder<Document> queryEncoder, final Encoder<T> encoder) {
        return getPrimary().replace(namespace, replace, queryEncoder, encoder);
    }

    @Override
    public WriteResult remove(final MongoNamespace namespace, final MongoRemove remove, final Encoder<Document> queryEncoder) {
        return getPrimary().remove(namespace, remove, queryEncoder);
   }

    @Override
    public Future<CommandResult> asyncCommand(final String database, final MongoCommand commandOperation,
                                              final Codec<Document> codec) {
        return getConnection(commandOperation.getReadPreference()).asyncCommand(database, commandOperation, codec);
    }

    @Override
    public void asyncCommand(final String database, final MongoCommand commandOperation, final Codec<Document> codec,
                             final SingleResultCallback<CommandResult> callback) {
        getConnection(commandOperation.getReadPreference()).asyncCommand(database, commandOperation, codec, callback);
    }

    @Override
    public <T> Future<QueryResult<T>> asyncQuery(final MongoNamespace namespace, final MongoFind find,
                                                 final Encoder<Document> queryEncoder, final Decoder<T> resultDecoder) {
        return getConnection(find.getReadPreference()).asyncQuery(namespace, find, queryEncoder, resultDecoder);
    }

    @Override
    public <T> void asyncQuery(final MongoNamespace namespace, final MongoFind find, final Encoder<Document> queryEncoder,
                               final Decoder<T> resultDecoder, final SingleResultCallback<QueryResult<T>> callback) {
        getConnection(find.getReadPreference()).asyncQuery(namespace, find, queryEncoder, resultDecoder, callback);
    }

    @Override
    public <T> Future<QueryResult<T>> asyncGetMore(final MongoNamespace namespace, final GetMore getMore,
                                                   final Decoder<T> resultDecoder) {
        return getConnection(getMore.getServerCursor().getAddress()).asyncGetMore(namespace, getMore, resultDecoder);
    }

    @Override
    public <T> void asyncGetMore(final MongoNamespace namespace, final GetMore getMore, final Decoder<T> resultDecoder,
                                 final SingleResultCallback<QueryResult<T>> callback) {
        getConnection(getMore.getServerCursor().getAddress()).asyncGetMore(namespace, getMore, resultDecoder, callback);
    }

    @Override
    public <T> Future<WriteResult> asyncInsert(final MongoNamespace namespace, final MongoInsert<T> insert,
                                               final Encoder<T> encoder) {
        return getPrimary().asyncInsert(namespace, insert, encoder);
    }

    @Override
    public <T> void asyncInsert(final MongoNamespace namespace, final MongoInsert<T> insert,
                                final Encoder<T> encoder,
                                final SingleResultCallback<WriteResult> callback) {
        getPrimary().asyncInsert(namespace, insert, encoder, callback);
    }

    @Override
    public Future<WriteResult> asyncUpdate(final MongoNamespace namespace, final MongoUpdate update,
                                           final Encoder<Document> queryEncoder) {
        return getPrimary().asyncUpdate(namespace, update, queryEncoder);
    }

    @Override
    public void asyncUpdate(final MongoNamespace namespace, final MongoUpdate update,
                            final Encoder<Document> queryEncoder,
                            final SingleResultCallback<WriteResult> callback) {
        getPrimary().asyncUpdate(namespace, update, queryEncoder, callback);
    }

    @Override
    public <T> Future<WriteResult> asyncReplace(final MongoNamespace namespace, final MongoReplace<T> replace,
                                                final Encoder<Document> queryEncoder,
                                                final Encoder<T> encoder) {
        return getPrimary().asyncReplace(namespace, replace, queryEncoder, encoder);
    }

    @Override
    public <T> void asyncReplace(final MongoNamespace namespace, final MongoReplace<T> replace,
                                 final Encoder<Document> queryEncoder, final Encoder<T> encoder,
                                 final SingleResultCallback<WriteResult> callback) {
        getPrimary().asyncReplace(namespace, replace, queryEncoder, encoder, callback);
    }

    @Override
    public Future<WriteResult> asyncRemove(final MongoNamespace namespace, final MongoRemove remove,
                                           final Encoder<Document> queryEncoder) {
        return getPrimary().asyncRemove(namespace, remove, queryEncoder);
    }

    @Override
    public void asyncRemove(final MongoNamespace namespace, final MongoRemove remove,
                            final Encoder<Document> queryEncoder,
                            final SingleResultCallback<WriteResult> callback) {
        getPrimary().asyncRemove(namespace, remove, queryEncoder, callback);
    }

    @Override
    public void close() {
        for (SingleServerMongoConnector cur : mongoClientMap.values()) {
            cur.close();
        }
        connectionStrategy.close();
    }

    @Override
    public List<ServerAddress> getServerAddressList() {
        return connectionStrategy.getAllAddresses();
    }

    SingleServerMongoConnector getPrimary() {
        final ServerAddress serverAddress = connectionStrategy.getAddressOfPrimary();
        return getConnection(serverAddress);
    }

    SingleServerMongoConnector getConnection(final ReadPreference readPreference) {
        final ServerAddress serverAddress = connectionStrategy.getAddressForReadPreference(readPreference);

        return getConnection(serverAddress);
    }

    private synchronized SingleServerMongoConnector getConnection(final ServerAddress serverAddress) {
        SingleServerMongoConnector connection = mongoClientMap.get(serverAddress);
        if (connection == null) {
            connection = MongoConnectionsImpl.create(serverAddress, options, bufferPool);
            mongoClientMap.put(serverAddress, connection);
        }
        return connection;
    }
}
