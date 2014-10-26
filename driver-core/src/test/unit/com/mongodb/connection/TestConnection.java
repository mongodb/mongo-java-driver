/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb.connection;

import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.WriteConcernResult;
import com.mongodb.async.MongoFuture;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.DeleteRequest;
import com.mongodb.bulk.InsertRequest;
import com.mongodb.bulk.UpdateRequest;
import org.bson.BsonDocument;
import org.bson.ByteBuf;
import org.bson.FieldNameValidator;
import org.bson.codecs.Decoder;

import java.util.List;

@SuppressWarnings("rawtypes")
class TestConnection implements Connection {
    private final InternalConnection internalConnection;
    private final ProtocolExecutor executor;
    private Protocol enqueuedProtocol;

    public TestConnection(final InternalConnection internalConnection, final ProtocolExecutor executor) {
        this.internalConnection = internalConnection;
        this.executor = executor;
    }

    @Override
    public int getCount() {
        return 1;
    }

    @Override
    public Connection retain() {
        return this;
    }

    @Override
    public void release() {

    }

    @Override
    public ConnectionDescription getDescription() {
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    @Override
    public WriteConcernResult insert(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                                     final List<InsertRequest> inserts) {
        return executeEnqueuedProtocol();
    }

    @Override
    public MongoFuture<WriteConcernResult> insertAsync(final MongoNamespace namespace, final boolean ordered,
                                                       final WriteConcern writeConcern,
                                                       final List<InsertRequest> inserts) {
        return executeEnqueuedProtocolAsync();
    }

    @Override
    public WriteConcernResult update(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                                     final List<UpdateRequest> updates) {
        return executeEnqueuedProtocol();
    }

    @Override
    public MongoFuture<WriteConcernResult> updateAsync(final MongoNamespace namespace, final boolean ordered,
                                                       final WriteConcern writeConcern,
                                                       final List<UpdateRequest> updates) {
        return executeEnqueuedProtocolAsync();
    }

    @Override
    public WriteConcernResult delete(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                                     final List<DeleteRequest> deletes) {
        return executeEnqueuedProtocol();
    }

    @Override
    public MongoFuture<WriteConcernResult> deleteAsync(final MongoNamespace namespace, final boolean ordered,
                                                       final WriteConcern writeConcern,
                                                       final List<DeleteRequest> deletes) {
        return executeEnqueuedProtocolAsync();
    }

    @Override
    public BulkWriteResult insertCommand(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                                         final List<InsertRequest> inserts) {
        return executeEnqueuedProtocol();
    }

    @Override
    public MongoFuture<BulkWriteResult> insertCommandAsync(final MongoNamespace namespace, final boolean ordered,
                                                           final WriteConcern writeConcern,
                                                           final List<InsertRequest> inserts) {
        return executeEnqueuedProtocolAsync();
    }

    @Override
    public BulkWriteResult updateCommand(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                                         final List<UpdateRequest> updates) {
        return executeEnqueuedProtocol();
    }

    @Override
    public MongoFuture<BulkWriteResult> updateCommandAsync(final MongoNamespace namespace, final boolean ordered,
                                                           final WriteConcern writeConcern,
                                                           final List<UpdateRequest> updates) {
        return executeEnqueuedProtocolAsync();
    }

    @Override
    public BulkWriteResult deleteCommand(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                                         final List<DeleteRequest> deletes) {
        return executeEnqueuedProtocol();
    }

    @Override
    public MongoFuture<BulkWriteResult> deleteCommandAsync(final MongoNamespace namespace, final boolean ordered,
                                                           final WriteConcern writeConcern,
                                                           final List<DeleteRequest> deletes) {
        return executeEnqueuedProtocolAsync();
    }

    @Override
    public <T> T command(final String database, final BsonDocument command, final boolean slaveOk,
                         final FieldNameValidator fieldNameValidator,
                         final Decoder<T> commandResultDecoder) {
        return executeEnqueuedProtocol();
    }

    @Override
    public <T> MongoFuture<T> commandAsync(final String database, final BsonDocument command, final boolean slaveOk,
                                           final FieldNameValidator fieldNameValidator,
                                           final Decoder<T> commandResultDecoder) {
        return executeEnqueuedProtocolAsync();
    }

    @Override
    public <T> QueryResult<T> query(final MongoNamespace namespace, final BsonDocument queryDocument, final BsonDocument fields,
                                    final int numberToReturn, final int skip,
                                    final boolean slaveOk, final boolean tailableCursor, final boolean awaitData,
                                    final boolean noCursorTimeout, final boolean exhaust,
                                    final boolean partial, final boolean oplogReplay, final Decoder<T> resultDecoder) {
        return executeEnqueuedProtocol();
    }

    @Override
    public <T> MongoFuture<QueryResult<T>> queryAsync(final MongoNamespace namespace, final BsonDocument queryDocument,
                                                      final BsonDocument fields,
                                                      final int numberToReturn, final int skip, final boolean slaveOk,
                                                      final boolean tailableCursor,
                                                      final boolean awaitData, final boolean noCursorTimeout, final boolean exhaust,
                                                      final boolean partial,
                                                      final boolean oplogReplay, final Decoder<T> resultDecoder) {
        return executeEnqueuedProtocolAsync();
    }

    @Override
    public <T> QueryResult<T> getMore(final MongoNamespace namespace, final long cursorId, final int numberToReturn,
                                      final Decoder<T> resultDecoder) {
        return executeEnqueuedProtocol();
    }

    @Override
    public <T> MongoFuture<QueryResult<T>> getMoreAsync(final MongoNamespace namespace, final long cursorId, final int numberToReturn,
                                                        final Decoder<T> resultDecoder) {
        return executeEnqueuedProtocolAsync();
    }

    @Override
    public <T> QueryResult<T> getMoreReceive(final Decoder<T> resultDecoder, final int responseTo) {
        return executeEnqueuedProtocol();
    }

    @Override
    public <T> MongoFuture<QueryResult<T>> getMoreReceiveAsync(final Decoder<T> resultDecoder, final int responseTo) {
        return executeEnqueuedProtocolAsync();
    }

    @Override
    public void getMoreDiscard(final long cursorId, final int responseTo) {
        executeEnqueuedProtocol();
    }

    @Override
    public MongoFuture<Void> getMoreDiscardAsync(final long cursorId, final int responseTo) {
        return executeEnqueuedProtocolAsync();
    }

    @Override
    public void killCursor(final List<Long> cursors) {
        executeEnqueuedProtocol();
    }

    @Override
    public MongoFuture<Void> killCursorAsync(final List<Long> cursors) {
        return executeEnqueuedProtocolAsync();
    }

    @Override
    public ByteBuf getBuffer(final int size) {
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    @SuppressWarnings("unchecked")
    private <T> T executeEnqueuedProtocol() {
        return (T) executor.execute(enqueuedProtocol, internalConnection);
    }

    @SuppressWarnings("unchecked")
    private <T> T executeEnqueuedProtocolAsync() {
        return (T) executor.executeAsync(enqueuedProtocol, internalConnection);
    }

    void enqueueProtocol(final Protocol protocol) {
        enqueuedProtocol = protocol;
    }
}
