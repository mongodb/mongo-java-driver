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
import org.mongodb.MongoClient;
import org.mongodb.MongoClientOptions;
import org.mongodb.MongoNamespace;
import org.mongodb.MongoNoPrimaryException;
import org.mongodb.MongoOperations;
import org.mongodb.MongoReadPreferenceException;
import org.mongodb.ReadPreference;
import org.mongodb.ServerAddress;
import org.mongodb.async.MongoAsyncOperations;
import org.mongodb.async.SingleResultCallback;
import org.mongodb.operation.GetMore;
import org.mongodb.operation.MongoCommand;
import org.mongodb.operation.MongoFind;
import org.mongodb.operation.MongoInsert;
import org.mongodb.operation.MongoKillCursor;
import org.mongodb.operation.MongoRemove;
import org.mongodb.operation.MongoReplace;
import org.mongodb.operation.MongoUpdate;
import org.mongodb.result.CommandResult;
import org.mongodb.result.GetMoreResult;
import org.mongodb.result.QueryResult;
import org.mongodb.result.ServerCursor;
import org.mongodb.result.WriteResult;
import org.mongodb.rs.ReplicaSet;
import org.mongodb.rs.ReplicaSetMember;
import org.mongodb.serialization.Serializer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * A {@code MongoClient} implementation that is capable of providing a logical connection to a replica set.
 */
class ReplicaSetMongoClient extends AbstractMongoClient {
    private final List<ServerAddress> seedList;
    private final ReplicaSetMonitor replicaSetMonitor;
    private Map<ServerAddress, SingleServerMongoClient> mongoClientMap = new HashMap<ServerAddress, SingleServerMongoClient>();

    ReplicaSetMongoClient(final List<ServerAddress> seedList, final MongoClientOptions options) {
        super(options);
        this.seedList = Collections.unmodifiableList(new ArrayList<ServerAddress>(seedList));
        replicaSetMonitor = new ReplicaSetMonitor(seedList, this);
        replicaSetMonitor.start();
    }

    @Override
    public MongoOperations getOperations() {
        return new ReplicaSetMongoOperations();
    }

    @Override
    public MongoAsyncOperations getAsyncOperations() {
        return new ReplicaSetMongoOperations();
    }

    @Override
    public void withConnection(final Runnable runnable) {
        getPrimary().withConnection(runnable);
    }

    @Override
    public <T> T withConnection(final Callable<T> callable) throws ExecutionException {
        return getPrimary().withConnection(callable);
    }

    @Override
    public void close() {
        replicaSetMonitor.close();
        for (MongoClient cur : mongoClientMap.values()) {
            cur.close();
        }
    }

    @Override
    void bindToConnection() {
        getPrimary().bindToConnection();
    }

    @Override
    void unbindFromConnection() {
        getPrimary().bindToConnection();
    }

    @Override
    List<ServerAddress> getServerAddressList() {
        return seedList;
    }

    SingleServerMongoClient getPrimary() {
        ReplicaSet currentState = replicaSetMonitor.getCurrentState();
        ReplicaSetMember primary = currentState.getPrimary();
        if (primary == null) {
            throw new MongoNoPrimaryException(currentState);
        }
        return getClient(primary.getServerAddress());
    }

    SingleServerMongoClient getClient(final ReadPreference readPreference) {
        // TODO: this is hiding potential bugs.  ReadPrefence should not be null
        ReadPreference appliedReadPreference = readPreference == null ? ReadPreference.primary() : readPreference;
        final ReplicaSet replicaSet = replicaSetMonitor.getCurrentState();
        final ReplicaSetMember replicaSetMember = appliedReadPreference.chooseReplicaSetMember(replicaSet);
        if (replicaSetMember == null) {
            throw new MongoReadPreferenceException(readPreference, replicaSet);
        }
        return getClient(replicaSetMember.getServerAddress());
    }

    private synchronized SingleServerMongoClient getClient(final ServerAddress serverAddress) {
        SingleServerMongoClient client = mongoClientMap.get(serverAddress);
        if (client == null) {
           client = MongoClientsImpl.create(serverAddress, getOptions(), getBufferPool());
           mongoClientMap.put(serverAddress, client);
        }
        return client;
    }

    private class ReplicaSetMongoOperations implements MongoOperations, MongoAsyncOperations {
        @Override
        public CommandResult executeCommand(final String database, final MongoCommand commandOperation,
                                            final Serializer<Document> serializer) {
            return getClient(commandOperation.getReadPreference()).getOperations().executeCommand(database, commandOperation, serializer);
        }

        @Override
        public <T> QueryResult<T> query(final MongoNamespace namespace, final MongoFind find,
                                        final Serializer<Document> baseSerializer, final Serializer<T> serializer) {
            return getClient(find.getReadPreference()).getOperations().query(namespace, find, baseSerializer, serializer);
        }

        @Override
        public <T> GetMoreResult<T> getMore(final MongoNamespace namespace, final GetMore getMore, final Serializer<T> serializer) {
            return getClient(getMore.getServerCursor().getAddress()).getOperations().getMore(namespace, getMore, serializer);
        }

        @Override
        public void killCursors(final MongoKillCursor killCursor) {
            for (ServerCursor cursor : killCursor.getServerCursors()) {
                getClient(cursor.getAddress()).getOperations().killCursors(new MongoKillCursor(cursor));
            }
        }

        @Override
        public <T> WriteResult insert(final MongoNamespace namespace, final MongoInsert<T> insert, final Serializer<T> serializer,
                                      final Serializer<Document> baseSerializer) {
            return getPrimary().getOperations().insert(namespace, insert, serializer, baseSerializer);
        }

        @Override
        public WriteResult update(final MongoNamespace namespace, final MongoUpdate update, final Serializer<Document> serializer) {
            return getPrimary().getOperations().update(namespace, update, serializer);
        }

        @Override
        public <T> WriteResult replace(final MongoNamespace namespace, final MongoReplace<T> replace,
                                       final Serializer<Document> baseSerializer, final Serializer<T> serializer) {
            return getPrimary().getOperations().replace(namespace, replace, baseSerializer, serializer);
        }

        @Override
        public WriteResult remove(final MongoNamespace namespace, final MongoRemove remove, final Serializer<Document> serializer) {
            return getPrimary().getOperations().remove(namespace, remove, serializer);
        }

        @Override
        public Future<CommandResult> asyncExecuteCommand(final String database, final MongoCommand commandOperation,
                                                         final Serializer<Document> serializer) {
            return getClient(commandOperation.getReadPreference()).
                    getAsyncOperations().asyncExecuteCommand(database, commandOperation, serializer);
        }

        @Override
        public void asyncExecuteCommand(final String database, final MongoCommand commandOperation,
                                        final Serializer<Document> serializer, final SingleResultCallback<CommandResult> callback) {
            getClient(commandOperation.getReadPreference()).
                    getAsyncOperations().asyncExecuteCommand(database, commandOperation, serializer, callback);
        }

        @Override
        public <T> Future<QueryResult<T>> asyncQuery(final MongoNamespace namespace, final MongoFind find,
                                                     final Serializer<Document> baseSerializer, final Serializer<T> serializer) {
            return getClient(find.getReadPreference()).getAsyncOperations().asyncQuery(namespace, find, baseSerializer, serializer);
        }

        @Override
        public <T> void asyncQuery(final MongoNamespace namespace, final MongoFind find, final Serializer<Document> baseSerializer,
                                   final Serializer<T> serializer, final SingleResultCallback<QueryResult<T>> callback) {
            getClient(find.getReadPreference()).getAsyncOperations().asyncQuery(namespace, find, baseSerializer, serializer, callback);
        }

        @Override
        public <T> Future<GetMoreResult<T>> asyncGetMore(final MongoNamespace namespace, final GetMore getMore,
                                                         final Serializer<T> serializer) {
            return getClient(getMore.getServerCursor().getAddress()).getAsyncOperations().asyncGetMore(namespace, getMore, serializer);
        }

        @Override
        public <T> void asyncGetMore(final MongoNamespace namespace, final GetMore getMore, final Serializer<T> serializer,
                                     final SingleResultCallback<GetMoreResult<T>> callback) {
            getClient(getMore.getServerCursor().getAddress()).getAsyncOperations().asyncGetMore(namespace, getMore, serializer, callback);
        }

        @Override
        public <T> Future<WriteResult> asyncInsert(final MongoNamespace namespace, final MongoInsert<T> insert,
                                                   final Serializer<T> serializer, final Serializer<Document> baseSerializer) {
            return getPrimary().getAsyncOperations().asyncInsert(namespace, insert, serializer, baseSerializer);
        }

        @Override
        public <T> void asyncInsert(final MongoNamespace namespace, final MongoInsert<T> insert, final Serializer<T> serializer,
                                    final Serializer<Document> baseSerializer, final SingleResultCallback<WriteResult> callback) {
            getPrimary().getAsyncOperations().asyncInsert(namespace, insert, serializer, baseSerializer, callback);
        }

        @Override
        public Future<WriteResult> asyncUpdate(final MongoNamespace namespace, final MongoUpdate update,
                                               final Serializer<Document> serializer) {
            return getPrimary().getAsyncOperations().asyncUpdate(namespace, update, serializer);
        }

        @Override
        public void asyncUpdate(final MongoNamespace namespace, final MongoUpdate update, final Serializer<Document> serializer,
                                final SingleResultCallback<WriteResult> callback) {
            getPrimary().getAsyncOperations().asyncUpdate(namespace, update, serializer, callback);
        }

        @Override
        public <T> Future<WriteResult> asyncReplace(final MongoNamespace namespace, final MongoReplace<T> replace,
                                                    final Serializer<Document> baseSerializer, final Serializer<T> serializer) {
            return getPrimary().getAsyncOperations().asyncReplace(namespace, replace, baseSerializer, serializer);
        }

        @Override
        public <T> void asyncReplace(final MongoNamespace namespace, final MongoReplace<T> replace,
                                     final Serializer<Document> baseSerializer, final Serializer<T> serializer,
                                     final SingleResultCallback<WriteResult> callback) {
            getPrimary().getAsyncOperations().asyncReplace(namespace, replace, baseSerializer, serializer, callback);
        }

        @Override
        public Future<WriteResult> asyncRemove(final MongoNamespace namespace, final MongoRemove remove,
                                               final Serializer<Document> serializer) {
            return getPrimary().getAsyncOperations().asyncRemove(namespace, remove, serializer);
        }

        @Override
        public void asyncRemove(final MongoNamespace namespace, final MongoRemove remove, final Serializer<Document> serializer,
                                final SingleResultCallback<WriteResult> callback) {
            getPrimary().getAsyncOperations().asyncRemove(namespace, remove, serializer, callback);
        }
    }
}
