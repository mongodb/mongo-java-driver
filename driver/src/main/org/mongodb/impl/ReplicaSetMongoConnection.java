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

import org.bson.types.Document;
import org.bson.util.BufferPool;
import org.mongodb.MongoClientOptions;
import org.mongodb.MongoConnection;
import org.mongodb.MongoNamespace;
import org.mongodb.MongoNoPrimaryException;
import org.mongodb.MongoReadPreferenceException;
import org.mongodb.ReadPreference;
import org.mongodb.ServerAddress;
import org.mongodb.async.SingleResultCallback;
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
import org.mongodb.rs.ReplicaSet;
import org.mongodb.rs.ReplicaSetMember;
import org.mongodb.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

class ReplicaSetMongoConnection implements MongoConnection {
    private final MongoClientOptions options;
    private final BufferPool<ByteBuffer> bufferPool;
    private final ReplicaSetMonitor replicaSetMonitor;
    private Map<ServerAddress, SingleServerMongoConnection> mongoClientMap =
            new HashMap<ServerAddress, SingleServerMongoConnection>();

    ReplicaSetMongoConnection(final List<ServerAddress> seedList, final MongoClientOptions options) {
        this.options = options;
        this.bufferPool = new PowerOfTwoByteBufferPool();
        replicaSetMonitor = new ReplicaSetMonitor(seedList, this);
        replicaSetMonitor.start();
    }

    @Override
    public CommandResult executeCommand(final String database, final MongoCommand commandOperation,
                                        final Serializer<Document> serializer) {
        return getConnection(commandOperation.getReadPreference()).executeCommand(database, commandOperation, serializer);
    }

    @Override
    public <T> QueryResult<T> query(final MongoNamespace namespace, final MongoFind find,
                                    final Serializer<Document> baseSerializer, final Serializer<T> serializer) {
        return getConnection(find.getReadPreference()).query(namespace, find, baseSerializer, serializer);
    }

    @Override
    public <T> QueryResult<T> getMore(final MongoNamespace namespace, final GetMore getMore,
                                      final Serializer<T> serializer) {
        return getConnection(getMore.getServerCursor().getAddress()).getMore(namespace, getMore, serializer);
    }

    @Override
    public void killCursors(final MongoKillCursor killCursor) {
        for (ServerCursor cursor : killCursor.getServerCursors()) {
            getConnection(cursor.getAddress()).killCursors(new MongoKillCursor(cursor));
        }
    }

    @Override
    public <T> WriteResult insert(final MongoNamespace namespace, final MongoInsert<T> insert,
                                  final Serializer<T> serializer,
                                  final Serializer<Document> baseSerializer) {
        return getPrimary().insert(namespace, insert, serializer, baseSerializer);
    }

    @Override
    public WriteResult update(final MongoNamespace namespace, final MongoUpdate update,
                              final Serializer<Document> serializer) {
        return getPrimary().update(namespace, update, serializer);
    }

    @Override
    public <T> WriteResult replace(final MongoNamespace namespace, final MongoReplace<T> replace,
                                   final Serializer<Document> baseSerializer, final Serializer<T> serializer) {
        return getPrimary().replace(namespace, replace, baseSerializer, serializer);
    }

    @Override
    public WriteResult remove(final MongoNamespace namespace, final MongoRemove remove,
                              final Serializer<Document> serializer) {
        return getPrimary().remove(namespace, remove, serializer);
    }

    @Override
    public Future<CommandResult> asyncExecuteCommand(final String database, final MongoCommand commandOperation,
                                                     final Serializer<Document> serializer) {
        return getConnection(commandOperation.getReadPreference()).asyncExecuteCommand(database, commandOperation, serializer);
    }

    @Override
    public void asyncExecuteCommand(final String database, final MongoCommand commandOperation,
                                    final Serializer<Document> serializer,
                                    final SingleResultCallback<CommandResult> callback) {
        getConnection(commandOperation.getReadPreference()).asyncExecuteCommand(database, commandOperation, serializer, callback);
    }

    @Override
    public <T> Future<QueryResult<T>> asyncQuery(final MongoNamespace namespace, final MongoFind find,
                                                 final Serializer<Document> baseSerializer,
                                                 final Serializer<T> serializer) {
        return getConnection(find.getReadPreference()).asyncQuery(namespace, find, baseSerializer, serializer);
    }

    @Override
    public <T> void asyncQuery(final MongoNamespace namespace, final MongoFind find,
                               final Serializer<Document> baseSerializer,
                               final Serializer<T> serializer, final SingleResultCallback<QueryResult<T>> callback) {
        getConnection(find.getReadPreference()).asyncQuery(namespace, find, baseSerializer, serializer, callback);
    }

    @Override
    public <T> Future<QueryResult<T>> asyncGetMore(final MongoNamespace namespace, final GetMore getMore,
                                                   final Serializer<T> serializer) {
        return getConnection(getMore.getServerCursor().getAddress()).asyncGetMore(namespace, getMore, serializer);
    }

    @Override
    public <T> void asyncGetMore(final MongoNamespace namespace, final GetMore getMore, final Serializer<T> serializer,
                                 final SingleResultCallback<QueryResult<T>> callback) {
        getConnection(getMore.getServerCursor().getAddress()).asyncGetMore(namespace, getMore, serializer, callback);
    }

    @Override
    public <T> Future<WriteResult> asyncInsert(final MongoNamespace namespace, final MongoInsert<T> insert,
                                               final Serializer<T> serializer,
                                               final Serializer<Document> baseSerializer) {
        return getPrimary().asyncInsert(namespace, insert, serializer, baseSerializer);
    }

    @Override
    public <T> void asyncInsert(final MongoNamespace namespace, final MongoInsert<T> insert,
                                final Serializer<T> serializer,
                                final Serializer<Document> baseSerializer,
                                final SingleResultCallback<WriteResult> callback) {
        getPrimary().asyncInsert(namespace, insert, serializer, baseSerializer, callback);
    }

    @Override
    public Future<WriteResult> asyncUpdate(final MongoNamespace namespace, final MongoUpdate update,
                                           final Serializer<Document> serializer) {
        return getPrimary().asyncUpdate(namespace, update, serializer);
    }

    @Override
    public void asyncUpdate(final MongoNamespace namespace, final MongoUpdate update,
                            final Serializer<Document> serializer,
                            final SingleResultCallback<WriteResult> callback) {
        getPrimary().asyncUpdate(namespace, update, serializer, callback);
    }

    @Override
    public <T> Future<WriteResult> asyncReplace(final MongoNamespace namespace, final MongoReplace<T> replace,
                                                final Serializer<Document> baseSerializer,
                                                final Serializer<T> serializer) {
        return getPrimary().asyncReplace(namespace, replace, baseSerializer, serializer);
    }

    @Override
    public <T> void asyncReplace(final MongoNamespace namespace, final MongoReplace<T> replace,
                                 final Serializer<Document> baseSerializer, final Serializer<T> serializer,
                                 final SingleResultCallback<WriteResult> callback) {
        getPrimary().asyncReplace(namespace, replace, baseSerializer, serializer, callback);
    }

    @Override
    public Future<WriteResult> asyncRemove(final MongoNamespace namespace, final MongoRemove remove,
                                           final Serializer<Document> serializer) {
        return getPrimary().asyncRemove(namespace, remove, serializer);
    }

    @Override
    public void asyncRemove(final MongoNamespace namespace, final MongoRemove remove,
                            final Serializer<Document> serializer,
                            final SingleResultCallback<WriteResult> callback) {
        getPrimary().asyncRemove(namespace, remove, serializer, callback);
    }

    @Override
    public void close() {
        replicaSetMonitor.close();
        for (SingleServerMongoConnection cur : mongoClientMap.values()) {
            cur.close();
        }
    }

    @Override
    public List<ServerAddress> getServerAddressList() {
        // TODO: get this from current ReplicaSetMonitor state
        throw new UnsupportedOperationException();
    }

    SingleServerMongoConnection getPrimary() {
        ReplicaSet currentState = replicaSetMonitor.getCurrentState();
        ReplicaSetMember primary = currentState.getPrimary();
        if (primary == null) {
            throw new MongoNoPrimaryException(currentState);
        }
        return getConnection(primary.getServerAddress());
    }

    SingleServerMongoConnection getConnection(final ReadPreference readPreference) {
        // TODO: this is hiding potential bugs.  ReadPreference should not be null
        ReadPreference appliedReadPreference = readPreference == null ? ReadPreference.primary() : readPreference;
        final ReplicaSet replicaSet = replicaSetMonitor.getCurrentState();
        final ReplicaSetMember replicaSetMember = appliedReadPreference.chooseReplicaSetMember(replicaSet);
        if (replicaSetMember == null) {
            throw new MongoReadPreferenceException(readPreference, replicaSet);
        }
        return getConnection(replicaSetMember.getServerAddress());
    }

    private synchronized SingleServerMongoConnection getConnection(final ServerAddress serverAddress) {
        SingleServerMongoConnection connection = mongoClientMap.get(serverAddress);
        if (connection == null) {
            connection = MongoConnectionsImpl.create(serverAddress, options, bufferPool);
            mongoClientMap.put(serverAddress, connection);
        }
        return connection;
    }

}
