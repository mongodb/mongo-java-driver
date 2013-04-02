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
import org.mongodb.MongoConnectionStrategy;
import org.mongodb.MongoConnector;
import org.mongodb.io.BufferPool;
import org.mongodb.MongoClientOptions;
import org.mongodb.MongoNamespace;
import org.mongodb.ReadPreference;
import org.mongodb.ServerAddress;
import org.mongodb.async.SingleResultCallback;
import org.mongodb.io.PowerOfTwoByteBufferPool;
import org.mongodb.operation.MongoGetMore;
import org.mongodb.command.MongoCommand;
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
import org.mongodb.serialization.Serializer;

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
                                 final Serializer<Document> serializer) {
        return getConnection(commandOperation.getReadPreference()).command(database, commandOperation, serializer);
    }

    @Override
    public <T> QueryResult<T> query(final MongoNamespace namespace, final MongoFind find,
                                    final Serializer<Document> querySerializer, final Serializer<T> resultSerializer) {
        return getConnection(find.getReadPreference()).query(namespace, find, querySerializer, resultSerializer);
    }

    @Override
    public <T> QueryResult<T> getMore(final MongoNamespace namespace, final MongoGetMore mongoGetMore,
                                      final Serializer<T> resultSerializer) {
        return getConnection(mongoGetMore.getServerCursor().getAddress()).getMore(namespace, mongoGetMore, resultSerializer);
    }

    @Override
    public void killCursors(final MongoKillCursor killCursor) {
        for (ServerCursor cursor : killCursor.getServerCursors()) {
            getConnection(cursor.getAddress()).killCursors(new MongoKillCursor(cursor));
        }
    }

    @Override
    public <T> WriteResult insert(final MongoNamespace namespace, final MongoInsert<T> insert, final Serializer<T> serializer) {
        return getPrimary().insert(namespace, insert, serializer);
    }

    @Override
    public WriteResult update(final MongoNamespace namespace, final MongoUpdate update, final Serializer<Document> querySerializer) {
        return getPrimary().update(namespace, update, querySerializer);
    }

    @Override
    public <T> WriteResult replace(final MongoNamespace namespace, final MongoReplace<T> replace,
                                   final Serializer<Document> querySerializer, final Serializer<T> serializer) {
        return getPrimary().replace(namespace, replace, querySerializer, serializer);
    }

    @Override
    public WriteResult remove(final MongoNamespace namespace, final MongoRemove remove, final Serializer<Document> querySerializer) {
        return getPrimary().remove(namespace, remove, querySerializer);
   }

    @Override
    public Future<CommandResult> asyncCommand(final String database, final MongoCommand commandOperation,
                                              final Serializer<Document> serializer) {
        return getConnection(commandOperation.getReadPreference()).asyncCommand(database, commandOperation, serializer);
    }

    @Override
    public void asyncCommand(final String database, final MongoCommand commandOperation, final Serializer<Document> serializer,
                             final SingleResultCallback<CommandResult> callback) {
        getConnection(commandOperation.getReadPreference()).asyncCommand(database, commandOperation, serializer, callback);
    }

    @Override
    public <T> Future<QueryResult<T>> asyncQuery(final MongoNamespace namespace, final MongoFind find,
                                                 final Serializer<Document> querySerializer, final Serializer<T> resultSerializer) {
        return getConnection(find.getReadPreference()).asyncQuery(namespace, find, querySerializer, resultSerializer);
    }

    @Override
    public <T> void asyncQuery(final MongoNamespace namespace, final MongoFind find, final Serializer<Document> querySerializer,
                               final Serializer<T> resultSerializer, final SingleResultCallback<QueryResult<T>> callback) {
        getConnection(find.getReadPreference()).asyncQuery(namespace, find, querySerializer, resultSerializer, callback);
    }

    @Override
    public <T> Future<QueryResult<T>> asyncGetMore(final MongoNamespace namespace, final MongoGetMore mongoGetMore,
                                                   final Serializer<T> resultSerializer) {
        return getConnection(mongoGetMore.getServerCursor().getAddress()).asyncGetMore(namespace, mongoGetMore, resultSerializer);
    }

    @Override
    public <T> void asyncGetMore(final MongoNamespace namespace, final MongoGetMore mongoGetMore, final Serializer<T> resultSerializer,
                                 final SingleResultCallback<QueryResult<T>> callback) {
        getConnection(mongoGetMore.getServerCursor().getAddress()).asyncGetMore(namespace, mongoGetMore, resultSerializer, callback);
    }

    @Override
    public <T> Future<WriteResult> asyncInsert(final MongoNamespace namespace, final MongoInsert<T> insert,
                                               final Serializer<T> serializer) {
        return getPrimary().asyncInsert(namespace, insert, serializer);
    }

    @Override
    public <T> void asyncInsert(final MongoNamespace namespace, final MongoInsert<T> insert,
                                final Serializer<T> serializer,
                                final SingleResultCallback<WriteResult> callback) {
        getPrimary().asyncInsert(namespace, insert, serializer, callback);
    }

    @Override
    public Future<WriteResult> asyncUpdate(final MongoNamespace namespace, final MongoUpdate update,
                                           final Serializer<Document> querySerializer) {
        return getPrimary().asyncUpdate(namespace, update, querySerializer);
    }

    @Override
    public void asyncUpdate(final MongoNamespace namespace, final MongoUpdate update,
                            final Serializer<Document> serializer,
                            final SingleResultCallback<WriteResult> callback) {
        getPrimary().asyncUpdate(namespace, update, serializer, callback);
    }

    @Override
    public <T> Future<WriteResult> asyncReplace(final MongoNamespace namespace, final MongoReplace<T> replace,
                                                final Serializer<Document> querySerializer,
                                                final Serializer<T> serializer) {
        return getPrimary().asyncReplace(namespace, replace, querySerializer, serializer);
    }

    @Override
    public <T> void asyncReplace(final MongoNamespace namespace, final MongoReplace<T> replace,
                                 final Serializer<Document> querySerializer, final Serializer<T> serializer,
                                 final SingleResultCallback<WriteResult> callback) {
        getPrimary().asyncReplace(namespace, replace, querySerializer, serializer, callback);
    }

    @Override
    public Future<WriteResult> asyncRemove(final MongoNamespace namespace, final MongoRemove remove,
                                           final Serializer<Document> querySerializer) {
        return getPrimary().asyncRemove(namespace, remove, querySerializer);
    }

    @Override
    public void asyncRemove(final MongoNamespace namespace, final MongoRemove remove,
                            final Serializer<Document> querySerializer,
                            final SingleResultCallback<WriteResult> callback) {
        getPrimary().asyncRemove(namespace, remove, querySerializer, callback);
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
