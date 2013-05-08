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

import org.mongodb.Codec;
import org.mongodb.CommandOperation;
import org.mongodb.Decoder;
import org.mongodb.Document;
import org.mongodb.Encoder;
import org.mongodb.GetMoreOperation;
import org.mongodb.InsertOperation;
import org.mongodb.KillCursorOperation;
import org.mongodb.MongoConnectionManager;
import org.mongodb.MongoConnector;
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;
import org.mongodb.MongoServerBinding;
import org.mongodb.QueryOperation;
import org.mongodb.RemoveOperation;
import org.mongodb.ReplaceOperation;
import org.mongodb.ServerAddress;
import org.mongodb.UpdateOperation;
import org.mongodb.command.MongoCommand;
import org.mongodb.io.BufferPool;
import org.mongodb.io.PowerOfTwoByteBufferPool;
import org.mongodb.operation.MongoFind;
import org.mongodb.operation.MongoGetMore;
import org.mongodb.operation.MongoInsert;
import org.mongodb.operation.MongoKillCursor;
import org.mongodb.operation.MongoRemove;
import org.mongodb.operation.MongoReplace;
import org.mongodb.operation.MongoUpdate;
import org.mongodb.result.CommandResult;
import org.mongodb.result.QueryResult;
import org.mongodb.result.ServerCursor;
import org.mongodb.result.WriteResult;

import java.nio.ByteBuffer;
import java.util.List;

class DelegatingMongoConnector implements MongoConnector {
    private final MongoServerBinding binding;

    public DelegatingMongoConnector(final MongoServerBinding connectorManager) {
        this.binding = connectorManager;
    }

    @Override
    public CommandResult command(final String database, final MongoCommand commandOperation,
                                 final Codec<Document> codec) {
        MongoConnectionManager connectionManager =
                binding.getConnectionManagerForRead(commandOperation.getReadPreference());
        MongoConnection connection = connectionManager.getConnection();
        try {
            return new CommandOperation(database, commandOperation, codec, connection.getBufferPool()).execute(connection.getGateway());
        } finally {
            connectionManager.releaseConnection(connection);
        }
    }

    @Override
    public <T> QueryResult<T> query(final MongoNamespace namespace, final MongoFind find,
                                    final Encoder<Document> queryEncoder, final Decoder<T> resultDecoder) {
        MongoConnectionManager connectionManager = binding.getConnectionManagerForRead(find.getReadPreference());
        MongoConnection connection = connectionManager.getConnection();
        try {
            return new QueryOperation<T>(namespace, find, queryEncoder, resultDecoder, connection.getBufferPool()).execute(connection.getGateway());
        } finally {
            connectionManager.releaseConnection(connection);
        }
    }

    @Override
    public <T> QueryResult<T> getMore(final MongoNamespace namespace, final MongoGetMore getMore,
                                      final Decoder<T> resultDecoder) {
        MongoConnectionManager connectionManager = binding.getConnectionManagerForServer(getMore.getServerCursor().getAddress());
        MongoConnection connection = connectionManager.getConnection();
        try {
            return new GetMoreOperation<T>(namespace, getMore, resultDecoder, connection.getBufferPool()).execute(connection.getGateway());
        } finally {
            connectionManager.releaseConnection(connection);
        }
    }

    @Override
    public void killCursors(final MongoKillCursor killCursor) {
        for (ServerCursor cursor : killCursor.getServerCursors()) {
            MongoConnectionManager connectionManager = binding.getConnectionManagerForServer(cursor.getAddress());
            MongoConnection connection = connectionManager.getConnection();
            try {
                new KillCursorOperation(new MongoKillCursor(cursor), connection.getBufferPool()).execute(connection.getGateway());
            } finally {
                connectionManager.releaseConnection(connection);
            }
        }
    }

    @Override
    public <T> WriteResult insert(final MongoNamespace namespace, final MongoInsert<T> insert, final Encoder<T> encoder) {
        MongoConnectionManager connectionManager = binding.getConnectionManagerForWrite();
        MongoConnection connection = connectionManager.getConnection();
        try {
            return new InsertOperation<T>(namespace, insert, encoder, connection.getBufferPool()).execute(connection.getGateway());
        } finally {
            connectionManager.releaseConnection(connection);
        }
    }

    @Override
    public WriteResult update(final MongoNamespace namespace, final MongoUpdate update, final Encoder<Document> queryEncoder) {
        MongoConnectionManager connectionManager = binding.getConnectionManagerForWrite();
        MongoConnection connection = connectionManager.getConnection();
        try {
            return new UpdateOperation(namespace, update, queryEncoder, connection.getBufferPool()).execute(connection.getGateway());
        } finally {
            connectionManager.releaseConnection(connection);
        }
    }

    @Override
    public <T> WriteResult replace(final MongoNamespace namespace, final MongoReplace<T> replace,
                                   final Encoder<Document> queryEncoder, final Encoder<T> encoder) {
        MongoConnectionManager connectionManager = binding.getConnectionManagerForWrite();
        MongoConnection connection = connectionManager.getConnection();
        try {
            return new ReplaceOperation<T>(namespace, replace, queryEncoder, encoder, connection.getBufferPool()).execute(connection.getGateway());
        } finally {
            connectionManager.releaseConnection(connection);
        }
    }

    @Override
    public WriteResult remove(final MongoNamespace namespace, final MongoRemove remove, final Encoder<Document> queryEncoder) {
        MongoConnectionManager connectionManager = binding.getConnectionManagerForWrite();
        MongoConnection connection = connectionManager.getConnection();
        try {
            return new RemoveOperation(namespace, remove, queryEncoder, connection.getBufferPool()).execute(connection.getGateway());
        } finally {
            connectionManager.releaseConnection(connection);
        }
    }

    @Override
    public MongoFuture<CommandResult> asyncCommand(final String database, final MongoCommand commandOperation,
                                                   final Codec<Document> codec) {
        MongoConnectionManager connectionManager = binding.getConnectionManagerForRead(commandOperation.getReadPreference());
        MongoConnection connection = connectionManager.getConnection();
        try {
            return connection.asyncCommand(database, commandOperation, codec);
        } finally {
            connectionManager.releaseConnection(connection);
        }
    }

    @Override
    public <T> MongoFuture<QueryResult<T>> asyncQuery(final MongoNamespace namespace, final MongoFind find,
                                                      final Encoder<Document> queryEncoder, final Decoder<T> resultDecoder) {
        MongoConnectionManager connectionManager = binding.getConnectionManagerForRead(find.getReadPreference());
        MongoConnection connection = connectionManager.getConnection();
        try {
            return connection.asyncQuery(namespace, find, queryEncoder, resultDecoder);
        } finally {
            connectionManager.releaseConnection(connection);
        }
    }

    @Override
    public <T> MongoFuture<QueryResult<T>> asyncGetMore(final MongoNamespace namespace, final MongoGetMore getMore,
                                                        final Decoder<T> resultDecoder) {
        MongoConnectionManager connectionManager =
                binding.getConnectionManagerForServer(getMore.getServerCursor().getAddress());
        MongoConnection connection =
                connectionManager.getConnection();
        try {
            return connection.asyncGetMore(namespace, getMore, resultDecoder);
        } finally {
            connectionManager.releaseConnection(connection);
        }
    }

    @Override
    public <T> MongoFuture<WriteResult> asyncInsert(final MongoNamespace namespace, final MongoInsert<T> insert,
                                                    final Encoder<T> encoder) {
        MongoConnectionManager connectionManager = binding.getConnectionManagerForWrite();
        MongoConnection connection = connectionManager.getConnection();
        try {
            return connection.asyncInsert(namespace, insert, encoder);
        } finally {
            connectionManager.releaseConnection(connection);
        }
    }

    @Override
    public MongoFuture<WriteResult> asyncUpdate(final MongoNamespace namespace, final MongoUpdate update,
                                                final Encoder<Document> queryEncoder) {
        MongoConnectionManager connectionManager = binding.getConnectionManagerForWrite();
        MongoConnection connection = connectionManager.getConnection();
        try {
            return connection.asyncUpdate(namespace, update, queryEncoder);
        } finally {
            connectionManager.releaseConnection(connection);
        }
    }

    @Override
    public <T> MongoFuture<WriteResult> asyncReplace(final MongoNamespace namespace, final MongoReplace<T> replace,
                                                     final Encoder<Document> queryEncoder,
                                                     final Encoder<T> encoder) {
        MongoConnectionManager connectionManager = binding.getConnectionManagerForWrite();
        MongoConnection connection = connectionManager.getConnection();
        try {
            return connection.asyncReplace(namespace, replace, queryEncoder, encoder);
        } finally {
            connectionManager.releaseConnection(connection);
        }
    }

    @Override
    public MongoFuture<WriteResult> asyncRemove(final MongoNamespace namespace, final MongoRemove remove,
                                                final Encoder<Document> queryEncoder) {
        MongoConnectionManager connectionManager = binding.getConnectionManagerForWrite();
        MongoConnection connection = connectionManager.getConnection();
        try {
            return connection.asyncRemove(namespace, remove, queryEncoder);
        } finally {
            connectionManager.releaseConnection(connection);
        }
    }

    @Override
    public void close() {
        binding.close();
    }

    @Override
    public MongoServerBinding getServerBinding() {
        return binding;
    }

    @Override
    public List<ServerAddress> getServerAddressList() {
        return binding.getAllServerAddresses();
    }

    @Override
    public MongoConnector getSession() {
        return new DelegatingMongoConnector(new MonotonicallyConsistentMongoServerBinding(getServerBinding()));
    }
}
