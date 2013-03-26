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
 *
 */

package org.mongodb.impl;

import org.mongodb.Document;
import org.mongodb.MongoClientOptions;
import org.mongodb.MongoConnector;
import org.mongodb.MongoNamespace;
import org.mongodb.ServerAddress;
import org.mongodb.async.SingleResultCallback;
import org.mongodb.operation.GetMore;
import org.mongodb.operation.MongoCommand;
import org.mongodb.operation.MongoFind;
import org.mongodb.operation.MongoInsert;
import org.mongodb.operation.MongoKillCursor;
import org.mongodb.operation.MongoRemove;
import org.mongodb.operation.MongoReplace;
import org.mongodb.operation.MongoUpdate;
import org.mongodb.pool.SimplePool;
import org.mongodb.result.CommandResult;
import org.mongodb.result.QueryResult;
import org.mongodb.result.WriteResult;
import org.mongodb.serialization.Serializer;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Future;

public class SingleServerMongoConnector implements MongoConnector {
    private final ThreadLocal<MongoPoolableConnector> boundClient = new ThreadLocal<MongoPoolableConnector>();
    private final MongoClientOptions options;
    private final ServerAddress serverAddress;
    private final SimplePool<MongoPoolableConnector> connectionPool;

    public SingleServerMongoConnector(final MongoClientOptions options, final ServerAddress serverAddress,
                                      final SimplePool<MongoPoolableConnector> connectionPool) {
        this.options = options;
        this.serverAddress = serverAddress;
        this.connectionPool = connectionPool;
    }

    public MongoClientOptions getOptions() {
        return options;
    }

    public ServerAddress getServerAddress() {
        return serverAddress;
    }

    @Override
    public List<ServerAddress> getServerAddressList() {
        return Arrays.asList(serverAddress);
    }

    @Override
    public void close() {
        connectionPool.close();
    }

    @Override
    public CommandResult command(final String database, final MongoCommand commandOperation,
                                 final Serializer<Document> serializer) {
        final MongoPoolableConnector connection = getChannelConnection();
        try {
            return connection.command(database, commandOperation, serializer);
        } finally {
            connection.release();
        }
    }

    @Override
    public <T> QueryResult<T> query(final MongoNamespace namespace, final MongoFind find,
                                    final Serializer<Document> querySerializer, final Serializer<T> resultSerializer) {
        final MongoPoolableConnector connection = getChannelConnection();
        try {
            return connection.query(namespace, find, querySerializer, resultSerializer);
        } finally {
            connection.release();
        }
    }

    @Override
    public <T> QueryResult<T> getMore(final MongoNamespace namespace, final GetMore getMore,
                                      final Serializer<T> resultSerializer) {
        final MongoPoolableConnector connection = getChannelConnection();
        try {
            return connection.getMore(namespace, getMore, resultSerializer);
        } finally {
            connection.release();
        }
    }

    @Override
    public <T> WriteResult insert(final MongoNamespace namespace, final MongoInsert<T> insert,
                                  final Serializer<T> serializer) {
        final MongoPoolableConnector connection = getChannelConnection();
        try {
            return connection.insert(namespace, insert, serializer);
        } finally {
            connection.release();
        }
    }

    @Override
    public WriteResult update(final MongoNamespace namespace, final MongoUpdate update,
                              final Serializer<Document> querySerializer) {
        final MongoPoolableConnector connection = getChannelConnection();
        try {
            return connection.update(namespace, update, querySerializer);
        } finally {
            connection.release();
        }
    }

    @Override
    public <T> WriteResult replace(final MongoNamespace namespace, final MongoReplace<T> replace,
                                   final Serializer<Document> querySerializer, final Serializer<T> serializer) {
        final MongoPoolableConnector connection = getChannelConnection();
        try {
            return connection.replace(namespace, replace, querySerializer, serializer);
        } finally {
            connection.release();
        }
    }

    @Override
    public WriteResult remove(final MongoNamespace namespace, final MongoRemove remove,
                              final Serializer<Document> querySerializer) {
        final MongoPoolableConnector connection = getChannelConnection();
        try {
            return connection.remove(namespace, remove, querySerializer);
        } finally {
            connection.release();
        }
    }

    @Override
    public void killCursors(final MongoKillCursor killCursor) {
        final MongoPoolableConnector connection = getChannelConnection();
        try {
            connection.killCursors(killCursor);
        } finally {
            connection.release();
        }
    }

    @Override
    public Future<CommandResult> asyncCommand(final String database, final MongoCommand commandOperation,
                                              final Serializer<Document> serializer) {
        final MongoPoolableConnector connection = getChannelConnection();
        try {
            return connection.asyncCommand(database, commandOperation, serializer);
        } finally {
            connection.release();
        }
    }

    @Override
    public void asyncCommand(final String database, final MongoCommand commandOperation,
                             final Serializer<Document> serializer,
                             final SingleResultCallback<CommandResult> callback) {
        final MongoPoolableConnector connection = getChannelConnection();
        try {
            connection.asyncCommand(database, commandOperation, serializer, callback);
        } finally {
            connection.release();
        }
    }

    @Override
    public <T> Future<QueryResult<T>> asyncQuery(final MongoNamespace namespace, final MongoFind find,
                                                 final Serializer<Document> querySerializer,
                                                 final Serializer<T> resultSerializer) {
        final MongoPoolableConnector connection = getChannelConnection();
        try {
            return connection.asyncQuery(namespace, find, querySerializer, resultSerializer);
        } finally {
            connection.release();
        }
    }

    @Override
    public <T> void asyncQuery(final MongoNamespace namespace, final MongoFind find,
                               final Serializer<Document> querySerializer,
                               final Serializer<T> resultSerializer,
                               final SingleResultCallback<QueryResult<T>> callback) {
        final MongoPoolableConnector connection = getChannelConnection();
        try {
            connection.asyncQuery(namespace, find, querySerializer, resultSerializer, callback);
        } finally {
            connection.release();
        }
    }

    @Override
    public <T> Future<QueryResult<T>> asyncGetMore(final MongoNamespace namespace, final GetMore getMore,
                                                   final Serializer<T> resultSerializer) {
        final MongoPoolableConnector connection = getChannelConnection();
        try {
            return connection.asyncGetMore(namespace, getMore, resultSerializer);
        } finally {
            connection.release();
        }
    }

    @Override
    public <T> void asyncGetMore(final MongoNamespace namespace, final GetMore getMore,
                                 final Serializer<T> resultSerializer,
                                 final SingleResultCallback<QueryResult<T>> callback) {
        final MongoPoolableConnector connection = getChannelConnection();
        try {
            connection.asyncGetMore(namespace, getMore, resultSerializer, callback);
        } finally {
            connection.release();
        }
    }

    @Override
    public <T> Future<WriteResult> asyncInsert(final MongoNamespace namespace, final MongoInsert<T> insert,
                                               final Serializer<T> serializer) {
        final MongoPoolableConnector connection = getChannelConnection();
        try {
            return connection.asyncInsert(namespace, insert, serializer);
        } finally {
            connection.release();
        }
    }

    @Override
    public <T> void asyncInsert(final MongoNamespace namespace, final MongoInsert<T> insert,
                                final Serializer<T> serializer,
                                final SingleResultCallback<WriteResult> callback) {
        final MongoPoolableConnector connection = getChannelConnection();
        try {
            connection.asyncInsert(namespace, insert, serializer, callback);
        } finally {
            connection.release();
        }
    }

    @Override
    public Future<WriteResult> asyncUpdate(final MongoNamespace namespace, final MongoUpdate update,
                                           final Serializer<Document> querySerializer) {
        final MongoPoolableConnector connection = getChannelConnection();
        try {
            return connection.asyncUpdate(namespace, update, querySerializer);
        } finally {
            connection.release();
        }
    }

    @Override
    public void asyncUpdate(final MongoNamespace namespace, final MongoUpdate update,
                            final Serializer<Document> serializer,
                            final SingleResultCallback<WriteResult> callback) {
        final MongoPoolableConnector connection = getChannelConnection();
        try {
            connection.asyncUpdate(namespace, update, serializer, callback);
        } finally {
            connection.release();
        }
    }

    @Override
    public <T> Future<WriteResult> asyncReplace(final MongoNamespace namespace, final MongoReplace<T> replace,
                                                final Serializer<Document> querySerializer,
                                                final Serializer<T> serializer) {
        final MongoPoolableConnector connection = getChannelConnection();
        try {
            return connection.asyncReplace(namespace, replace, querySerializer, serializer);
        } finally {
            connection.release();
        }
    }

    @Override
    public <T> void asyncReplace(final MongoNamespace namespace, final MongoReplace<T> replace,
                                 final Serializer<Document> querySerializer, final Serializer<T> serializer,
                                 final SingleResultCallback<WriteResult> callback) {
        final MongoPoolableConnector connection = getChannelConnection();
        try {
            connection.asyncReplace(namespace, replace, querySerializer, serializer, callback);
        } finally {
            connection.release();
        }
    }

    @Override
    public Future<WriteResult> asyncRemove(final MongoNamespace namespace, final MongoRemove remove,
                                           final Serializer<Document> querySerializer) {
        final MongoPoolableConnector connection = getChannelConnection();
        try {
            return connection.asyncRemove(namespace, remove, querySerializer);
        } finally {
            connection.release();
        }
    }

    @Override
    public void asyncRemove(final MongoNamespace namespace, final MongoRemove remove,
                            final Serializer<Document> querySerializer,
                            final SingleResultCallback<WriteResult> callback) {
        final MongoPoolableConnector connection = getChannelConnection();
        try {
            connection.asyncRemove(namespace, remove, querySerializer, callback);
        } finally {
            connection.release();
        }
    }


    protected MongoPoolableConnector getChannelConnection() {
        if (getBoundConnection().get() != null) {
            return getBoundConnection().get();
        }
        return connectionPool.get();
    }

    protected void releaseChannelConnection(final MongoPoolableConnector connection) {
        if (getBoundConnection().get() != null) {
            if (getBoundConnection().get() != connection) {
                throw new IllegalArgumentException("Can't unbind from a different client than you are bound to");
            }
            getBoundConnection().remove();
        }
        else {
            connection.close();
        }
    }

    protected ThreadLocal<MongoPoolableConnector> getBoundConnection() {
        return boundClient;
    }
}
