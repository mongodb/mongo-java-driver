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

import org.bson.types.Document;
import org.bson.util.BufferPool;
import org.mongodb.MongoClientOptions;
import org.mongodb.MongoConnection;
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
import org.mongodb.result.CommandResult;
import org.mongodb.result.QueryResult;
import org.mongodb.result.WriteResult;
import org.mongodb.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Future;

public abstract class SingleServerMongoConnection implements MongoConnection {
    private final ThreadLocal<SingleChannelMongoConnection> boundClient = new ThreadLocal<SingleChannelMongoConnection>();
    private final MongoClientOptions options;
    private final BufferPool<ByteBuffer> bufferPool;
    private final ServerAddress serverAddress;

    public SingleServerMongoConnection(final MongoClientOptions options, final BufferPool<ByteBuffer> bufferPool,
                                       final ServerAddress serverAddress) {
        this.options = options;
        this.bufferPool = bufferPool;
        this.serverAddress = serverAddress;
    }

    public MongoClientOptions getOptions() {
        return options;
    }

    public BufferPool<ByteBuffer> getBufferPool() {
        return bufferPool;
    }

    public ServerAddress getServerAddress() {
        return serverAddress;
    }

    @Override
    public List<ServerAddress> getServerAddressList() {
        return Arrays.asList(serverAddress);
    }

    @Override
    public CommandResult executeCommand(final String database, final MongoCommand commandOperation,
                                        final Serializer<Document> serializer) {
        final MongoConnection connection = getChannelConnection();
        try {
            return connection.executeCommand(database, commandOperation, serializer);
        } finally {
            releaseChannelClient(connection);
        }
    }

    @Override
    public <T> QueryResult<T> query(final MongoNamespace namespace, final MongoFind find,
                                    final Serializer<Document> baseSerializer, final Serializer<T> serializer) {
        final MongoConnection connection = getChannelConnection();
        try {
            return connection.query(namespace, find, baseSerializer, serializer);
        } finally {
            releaseChannelClient(connection);
        }
    }

    @Override
    public <T> QueryResult<T> getMore(final MongoNamespace namespace, final GetMore getMore,
                                      final Serializer<T> serializer) {
        final MongoConnection connection = getChannelConnection();
        try {
            return connection.getMore(namespace, getMore, serializer);
        } finally {
            releaseChannelClient(connection);
        }
    }

    @Override
    public <T> WriteResult insert(final MongoNamespace namespace, final MongoInsert<T> insert,
                                  final Serializer<T> serializer, final Serializer<Document> baseSerializer) {
        final MongoConnection connection = getChannelConnection();
        try {
            return connection.insert(namespace, insert, serializer, baseSerializer);
        } finally {
            releaseChannelClient(connection);
        }
    }

    @Override
    public WriteResult update(final MongoNamespace namespace, final MongoUpdate update,
                              final Serializer<Document> serializer) {
        final MongoConnection connection = getChannelConnection();
        try {
            return connection.update(namespace, update, serializer);
        } finally {
            releaseChannelClient(connection);
        }
    }

    @Override
    public <T> WriteResult replace(final MongoNamespace namespace, final MongoReplace<T> replace,
                                   final Serializer<Document> baseSerializer, final Serializer<T> serializer) {
        final MongoConnection connection = getChannelConnection();
        try {
            return connection.replace(namespace, replace, baseSerializer, serializer);
        } finally {
            releaseChannelClient(connection);
        }
    }

    @Override
    public WriteResult remove(final MongoNamespace namespace, final MongoRemove remove,
                              final Serializer<Document> serializer) {
        final MongoConnection connection = getChannelConnection();
        try {
            return connection.remove(namespace, remove, serializer);
        } finally {
            releaseChannelClient(connection);
        }
    }

    @Override
    public void killCursors(final MongoKillCursor killCursor) {
        final MongoConnection connection = getChannelConnection();
        try {
            connection.killCursors(killCursor);
        } finally {
            releaseChannelClient(connection);
        }
    }

    @Override
    public Future<CommandResult> asyncExecuteCommand(final String database, final MongoCommand commandOperation,
                                                     final Serializer<Document> serializer) {
        final MongoConnection connection = getChannelConnection();
        try {
            return connection.asyncExecuteCommand(database, commandOperation, serializer);
        } finally {
            releaseChannelClient(connection);
        }
    }

    @Override
    public void asyncExecuteCommand(final String database, final MongoCommand commandOperation,
                                    final Serializer<Document> serializer,
                                    final SingleResultCallback<CommandResult> callback) {
        final MongoConnection connection = getChannelConnection();
        try {
            connection.asyncExecuteCommand(database, commandOperation, serializer, callback);
        } finally {
            releaseChannelClient(connection);
        }
    }

    @Override
    public <T> Future<QueryResult<T>> asyncQuery(final MongoNamespace namespace, final MongoFind find,
                                                 final Serializer<Document> baseSerializer,
                                                 final Serializer<T> serializer) {
        final MongoConnection connection = getChannelConnection();
        try {
            return connection.asyncQuery(namespace, find, baseSerializer, serializer);
        } finally {
            releaseChannelClient(connection);
        }
    }

    @Override
    public <T> void asyncQuery(final MongoNamespace namespace, final MongoFind find,
                               final Serializer<Document> baseSerializer,
                               final Serializer<T> serializer,
                               final SingleResultCallback<QueryResult<T>> callback) {
        final MongoConnection connection = getChannelConnection();
        try {
            connection.asyncQuery(namespace, find, baseSerializer, serializer, callback);
        } finally {
            releaseChannelClient(connection);
        }
    }

    @Override
    public <T> Future<QueryResult<T>> asyncGetMore(final MongoNamespace namespace, final GetMore getMore,
                                                   final Serializer<T> serializer) {
        final MongoConnection connection = getChannelConnection();
        try {
            return connection.asyncGetMore(namespace, getMore, serializer);
        } finally {
            releaseChannelClient(connection);
        }
    }

    @Override
    public <T> void asyncGetMore(final MongoNamespace namespace, final GetMore getMore,
                                 final Serializer<T> serializer,
                                 final SingleResultCallback<QueryResult<T>> callback) {
        final MongoConnection connection = getChannelConnection();
        try {
            connection.asyncGetMore(namespace, getMore, serializer, callback);
        } finally {
            releaseChannelClient(connection);
        }
    }

    @Override
    public <T> Future<WriteResult> asyncInsert(final MongoNamespace namespace, final MongoInsert<T> insert,
                                               final Serializer<T> serializer,
                                               final Serializer<Document> baseSerializer) {
        final MongoConnection connection = getChannelConnection();
        try {
            return connection.asyncInsert(namespace, insert, serializer, baseSerializer);
        } finally {
            releaseChannelClient(connection);
        }
    }

    @Override
    public <T> void asyncInsert(final MongoNamespace namespace, final MongoInsert<T> insert,
                                final Serializer<T> serializer,
                                final Serializer<Document> baseSerializer,
                                final SingleResultCallback<WriteResult> callback) {
        final MongoConnection connection = getChannelConnection();
        try {
            connection.asyncInsert(namespace, insert, serializer, baseSerializer, callback);
        } finally {
            releaseChannelClient(connection);
        }
    }

    @Override
    public Future<WriteResult> asyncUpdate(final MongoNamespace namespace, final MongoUpdate update,
                                           final Serializer<Document> serializer) {
        final MongoConnection connection = getChannelConnection();
        try {
            return connection.asyncUpdate(namespace, update, serializer);
        } finally {
            releaseChannelClient(connection);
        }
    }

    @Override
    public void asyncUpdate(final MongoNamespace namespace, final MongoUpdate update,
                            final Serializer<Document> serializer,
                            final SingleResultCallback<WriteResult> callback) {
        final MongoConnection connection = getChannelConnection();
        try {
            connection.asyncUpdate(namespace, update, serializer, callback);
        } finally {
            releaseChannelClient(connection);
        }
    }

    @Override
    public <T> Future<WriteResult> asyncReplace(final MongoNamespace namespace, final MongoReplace<T> replace,
                                                final Serializer<Document> baseSerializer,
                                                final Serializer<T> serializer) {
        final MongoConnection connection = getChannelConnection();
        try {
            return connection.asyncReplace(namespace, replace, baseSerializer, serializer);
        } finally {
            releaseChannelClient(connection);
        }
    }

    @Override
    public <T> void asyncReplace(final MongoNamespace namespace, final MongoReplace<T> replace,
                                 final Serializer<Document> baseSerializer, final Serializer<T> serializer,
                                 final SingleResultCallback<WriteResult> callback) {
        final MongoConnection connection = getChannelConnection();
        try {
            connection.asyncReplace(namespace, replace, baseSerializer, serializer, callback);
        } finally {
            releaseChannelClient(connection);
        }
    }

    @Override
    public Future<WriteResult> asyncRemove(final MongoNamespace namespace, final MongoRemove remove,
                                           final Serializer<Document> serializer) {
        final MongoConnection connection = getChannelConnection();
        try {
            return connection.asyncRemove(namespace, remove, serializer);
        } finally {
            releaseChannelClient(connection);
        }
    }

    @Override
    public void asyncRemove(final MongoNamespace namespace, final MongoRemove remove,
                            final Serializer<Document> serializer,
                            final SingleResultCallback<WriteResult> callback) {
        final MongoConnection connection = getChannelConnection();
        try {
            connection.asyncRemove(namespace, remove, serializer, callback);
        } finally {
            releaseChannelClient(connection);
        }
    }


    protected MongoConnection getChannelConnection() {
        if (getBoundClient().get() != null) {
            return getBoundClient().get();
        }
        return createSingleChannelMongoConnection();
    }

    protected abstract MongoConnection createSingleChannelMongoConnection();

    protected void releaseChannelClient(final MongoConnection mongoConnection) {
        if (getBoundClient().get() != null) {
            if (getBoundClient().get() != mongoConnection) {
                throw new IllegalArgumentException("Can't unbind from a different client than you are bound to");
            }
            getBoundClient().remove();
        }
        else {
            mongoConnection.close();
        }
    }

    protected ThreadLocal<SingleChannelMongoConnection> getBoundClient() {
        return boundClient;
    }
}
