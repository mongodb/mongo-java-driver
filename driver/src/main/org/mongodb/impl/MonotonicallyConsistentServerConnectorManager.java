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

import org.mongodb.Codec;
import org.mongodb.Decoder;
import org.mongodb.Document;
import org.mongodb.Encoder;
import org.mongodb.MongoConnector;
import org.mongodb.MongoNamespace;
import org.mongodb.PoolableConnectionManager;
import org.mongodb.ReadPreference;
import org.mongodb.ServerAddress;
import org.mongodb.ServerConnectorManager;
import org.mongodb.async.SingleResultCallback;
import org.mongodb.command.MongoCommand;
import org.mongodb.operation.MongoFind;
import org.mongodb.operation.MongoGetMore;
import org.mongodb.operation.MongoInsert;
import org.mongodb.operation.MongoKillCursor;
import org.mongodb.operation.MongoRemove;
import org.mongodb.operation.MongoReplace;
import org.mongodb.operation.MongoUpdate;
import org.mongodb.result.CommandResult;
import org.mongodb.result.QueryResult;
import org.mongodb.result.WriteResult;

import java.util.List;
import java.util.concurrent.Future;

public class MonotonicallyConsistentServerConnectorManager implements ServerConnectorManager {
    private final ServerConnectorManager serverConnectorManager;
    private ReadPreference lastRequestedReadPreference;
    private MongoPoolableConnector connectorForReads;
    private MongoPoolableConnector connectorForWrites;

    public MonotonicallyConsistentServerConnectorManager(final ServerConnectorManager serverConnectorManager) {
        this.serverConnectorManager = serverConnectorManager;
    }

    @Override
    public PoolableConnectionManager getConnectionManagerForWrite() {
        return new PoolableConnectionManagerForWrites();
    }

    @Override
    public PoolableConnectionManager getConnectionManagerForRead(final ReadPreference readPreference) {
        return new PoolableConnectionManagerForReads(readPreference);
    }

    @Override
    public PoolableConnectionManager getConnectionManagerForServer(final ServerAddress serverAddress) {
        return serverConnectorManager.getConnectionManagerForServer(serverAddress);
    }

    @Override
    public List<ServerAddress> getAllServerAddresses() {
        return serverConnectorManager.getAllServerAddresses();
    }

    @Override
    public void close() {
        if (connectorForReads != null) {
            connectorForReads.release();
        }
        if (connectorForWrites != null) {
            connectorForWrites.release();
        }
    }

    private synchronized MongoPoolableConnector getConnectorForWrites() {
        if (connectorForWrites == null) {
            connectorForWrites = new ReleaseIgnoringMongoPoolableConnector(
                    serverConnectorManager.getConnectionManagerForWrite().getConnection());
            if (connectorForReads != null) {
                connectorForReads.close();
                connectorForReads = null;
            }
        }
        return connectorForWrites;
    }

    private synchronized MongoPoolableConnector getConnectorForReads(final ReadPreference readPreference) {
        if (connectorForWrites != null) {
            return connectorForWrites;
        }
        else if (connectorForReads == null || !readPreference.equals(lastRequestedReadPreference)) {
            lastRequestedReadPreference = readPreference;
            if (connectorForReads != null) {
                connectorForReads.release();
            }
            connectorForReads = new ReleaseIgnoringMongoPoolableConnector(
                    serverConnectorManager.getConnectionManagerForRead(readPreference).getConnection());
        }
        return connectorForReads;
    }


    private abstract class AbstractConnectionManager implements PoolableConnectionManager {

        @Override
        public ServerAddress getServerAddress() {
            return getConnection().getServerAddressList().get(0);
        }

        @Override
        public void close() {
        }
    }

    private final class PoolableConnectionManagerForReads extends AbstractConnectionManager {
        private final ReadPreference readPreference;

        private PoolableConnectionManagerForReads(final ReadPreference readPreference) {
            this.readPreference = readPreference;
        }

        @Override
        public MongoPoolableConnector getConnection() {
            return getConnectorForReads(readPreference);
        }
    }

    private final class PoolableConnectionManagerForWrites extends AbstractConnectionManager {
        @Override
        public MongoPoolableConnector getConnection() {
            return getConnectorForWrites();
        }
    }

    private final class ReleaseIgnoringMongoPoolableConnector implements MongoPoolableConnector {
        private final MongoPoolableConnector proxy;

        private ReleaseIgnoringMongoPoolableConnector(final MongoPoolableConnector proxy) {
            this.proxy = proxy;
        }

        @Override
        public void release() {
        }

        @Override
        public void close() {
        }

        @Override
        public List<ServerAddress> getServerAddressList() {
            return proxy.getServerAddressList();
        }

        @Override
        public MongoConnector getSession() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Future<CommandResult> asyncCommand(final String database, final MongoCommand commandOperation,
                                                  final Codec<Document> codec) {
            return proxy.asyncCommand(database, commandOperation, codec);
        }

        @Override

        public void asyncCommand(final String database, final MongoCommand commandOperation, final Codec<Document> codec,
                                 final SingleResultCallback<CommandResult> callback) {
            proxy.asyncCommand(database, commandOperation, codec, callback);
        }

        @Override
        public <T> Future<QueryResult<T>> asyncQuery(final MongoNamespace namespace, final MongoFind find,
                                                     final Encoder<Document> queryEncoder, final Decoder<T> resultDecoder) {
            return proxy.asyncQuery(namespace, find, queryEncoder, resultDecoder);
        }

        @Override
        public <T> void asyncQuery(final MongoNamespace namespace, final MongoFind find, final Encoder<Document> queryEncoder,
                                   final Decoder<T> resultDecoder, final SingleResultCallback<QueryResult<T>> callback) {
            proxy.asyncQuery(namespace, find, queryEncoder, resultDecoder, callback);
        }

        @Override
        public <T> Future<QueryResult<T>> asyncGetMore(final MongoNamespace namespace, final MongoGetMore getMore,
                                                       final Decoder<T> resultDecoder) {
            return proxy.asyncGetMore(namespace, getMore, resultDecoder);
        }

        @Override
        public <T> void asyncGetMore(final MongoNamespace namespace, final MongoGetMore getMore, final Decoder<T> resultDecoder,
                                     final SingleResultCallback<QueryResult<T>> callback) {
            proxy.asyncGetMore(namespace, getMore, resultDecoder, callback);
        }

        @Override
        public <T> Future<WriteResult> asyncInsert(final MongoNamespace namespace, final MongoInsert<T> insert, final Encoder<T> encoder) {
            return proxy.asyncInsert(namespace, insert, encoder);
        }

        @Override
        public <T> void asyncInsert(final MongoNamespace namespace, final MongoInsert<T> insert, final Encoder<T> encoder,
                                    final SingleResultCallback<WriteResult> callback) {
            proxy.asyncInsert(namespace, insert, encoder, callback);
        }

        @Override
        public Future<WriteResult> asyncUpdate(final MongoNamespace namespace, final MongoUpdate update,
                                               final Encoder<Document> queryEncoder) {
            return proxy.asyncUpdate(namespace, update, queryEncoder);
        }

        @Override
        public void asyncUpdate(final MongoNamespace namespace, final MongoUpdate update, final Encoder<Document> queryEncoder,
                                final SingleResultCallback<WriteResult> callback) {
            proxy.asyncUpdate(namespace, update, queryEncoder, callback);
        }

        @Override
        public <T> Future<WriteResult> asyncReplace(final MongoNamespace namespace, final MongoReplace<T> replace,
                                                    final Encoder<Document> queryEncoder, final Encoder<T> encoder) {
            return proxy.asyncReplace(namespace, replace, queryEncoder, encoder);
        }

        @Override
        public <T> void asyncReplace(final MongoNamespace namespace, final MongoReplace<T> replace, final Encoder<Document> queryEncoder,
                                     final Encoder<T> encoder, final SingleResultCallback<WriteResult> callback) {
            proxy.asyncReplace(namespace, replace, queryEncoder, encoder, callback);
        }

        @Override
        public Future<WriteResult> asyncRemove(final MongoNamespace namespace, final MongoRemove remove,
                                               final Encoder<Document> queryEncoder) {
            return proxy.asyncRemove(namespace, remove, queryEncoder);
        }

        @Override
        public void asyncRemove(final MongoNamespace namespace, final MongoRemove remove, final Encoder<Document> queryEncoder,
                                final SingleResultCallback<WriteResult> callback) {
            proxy.asyncRemove(namespace, remove, queryEncoder, callback);
        }

        @Override
        public CommandResult command(final String database, final MongoCommand commandOperation, final Codec<Document> codec) {
            return proxy.command(database, commandOperation, codec);
        }

        @Override
        public <T> QueryResult<T> query(final MongoNamespace namespace, final MongoFind find, final Encoder<Document> queryEncoder,
                                        final Decoder<T> resultDecoder) {
            return proxy.query(namespace, find, queryEncoder, resultDecoder);
        }

        @Override
        public <T> QueryResult<T> getMore(final MongoNamespace namespace, final MongoGetMore getMore, final Decoder<T> resultDecoder) {
            return proxy.getMore(namespace, getMore, resultDecoder);
        }

        @Override
        public void killCursors(final MongoKillCursor killCursor) {
            proxy.killCursors(killCursor);
        }

        @Override
        public <T> WriteResult insert(final MongoNamespace namespace, final MongoInsert<T> insert, final Encoder<T> encoder) {
            return proxy.insert(namespace, insert, encoder);
        }

        @Override
        public WriteResult update(final MongoNamespace namespace, final MongoUpdate update, final Encoder<Document> queryEncoder) {
            return proxy.update(namespace, update, queryEncoder);
        }

        @Override
        public <T> WriteResult replace(final MongoNamespace namespace, final MongoReplace<T> replace,
                                       final Encoder<Document> queryEncoder, final Encoder<T> encoder) {
            return proxy.replace(namespace, replace, queryEncoder, encoder);
        }

        @Override
        public WriteResult remove(final MongoNamespace namespace, final MongoRemove remove, final Encoder<Document> queryEncoder) {
            return proxy.remove(namespace, remove, queryEncoder);
        }
    }
}
