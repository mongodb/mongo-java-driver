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
import org.bson.FieldNameValidator;
import org.bson.codecs.Decoder;

import java.util.List;

import static com.mongodb.assertions.Assertions.isTrue;

class DefaultServerConnection extends AbstractReferenceCounted implements Connection {
    private final InternalConnection wrapped;
    private final ProtocolExecutor protocolExecutor;

    public DefaultServerConnection(final InternalConnection wrapped, final ProtocolExecutor protocolExecutor) {
        this.wrapped = wrapped;
        this.protocolExecutor = protocolExecutor;
    }

    @Override
    public DefaultServerConnection retain() {
        super.retain();
        return this;
    }

    @Override
    public void release() {
        super.release();
        if (getCount() == 0) {
            wrapped.close();
        }
    }

    @Override
    public ConnectionDescription getDescription() {
        isTrue("open", getCount() > 0);
        return wrapped.getDescription();
    }

    @Override
    public WriteConcernResult insert(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                                     final List<InsertRequest> inserts) {
        return executeProtocol(new InsertProtocol(namespace, ordered, writeConcern, inserts));
    }

    @Override
    public MongoFuture<WriteConcernResult> insertAsync(final MongoNamespace namespace, final boolean ordered,
                                                       final WriteConcern writeConcern,
                                                       final List<InsertRequest> inserts) {
        return executeProtocolAsync(new InsertProtocol(namespace, ordered, writeConcern, inserts));
    }

    @Override
    public WriteConcernResult update(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                                     final List<UpdateRequest> updates) {
        return executeProtocol(new UpdateProtocol(namespace, ordered, writeConcern, updates));
    }

    @Override
    public MongoFuture<WriteConcernResult> updateAsync(final MongoNamespace namespace, final boolean ordered,
                                                       final WriteConcern writeConcern,
                                                       final List<UpdateRequest> updates) {
        return executeProtocolAsync(new UpdateProtocol(namespace, ordered, writeConcern, updates));
    }

    @Override
    public WriteConcernResult delete(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                                     final List<DeleteRequest> deletes) {
        return executeProtocol(new DeleteProtocol(namespace, ordered, writeConcern, deletes));
    }

    @Override
    public MongoFuture<WriteConcernResult> deleteAsync(final MongoNamespace namespace, final boolean ordered,
                                                       final WriteConcern writeConcern,
                                                       final List<DeleteRequest> deletes) {
        return executeProtocolAsync(new DeleteProtocol(namespace, ordered, writeConcern, deletes));
    }

    @Override
    public BulkWriteResult insertCommand(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                                         final List<InsertRequest> inserts) {
        return executeProtocol(new InsertCommandProtocol(namespace, ordered, writeConcern, inserts));
    }

    @Override
    public MongoFuture<BulkWriteResult> insertCommandAsync(final MongoNamespace namespace, final boolean ordered,
                                                           final WriteConcern writeConcern,
                                                           final List<InsertRequest> inserts) {
        return executeProtocolAsync(new InsertCommandProtocol(namespace, ordered, writeConcern, inserts));
    }

    @Override
    public BulkWriteResult updateCommand(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                                         final List<UpdateRequest> updates) {
        return executeProtocol(new UpdateCommandProtocol(namespace, ordered, writeConcern, updates));

    }

    @Override
    public MongoFuture<BulkWriteResult> updateCommandAsync(final MongoNamespace namespace, final boolean ordered,
                                                           final WriteConcern writeConcern,
                                                           final List<UpdateRequest> updates) {
        return executeProtocolAsync(new UpdateCommandProtocol(namespace, ordered, writeConcern, updates));
    }

    @Override
    public BulkWriteResult deleteCommand(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                                         final List<DeleteRequest> deletes) {
        return executeProtocol(new DeleteCommandProtocol(namespace, ordered, writeConcern, deletes));

    }

    @Override
    public MongoFuture<BulkWriteResult> deleteCommandAsync(final MongoNamespace namespace, final boolean ordered,
                                                           final WriteConcern writeConcern,
                                                           final List<DeleteRequest> deletes) {
        return executeProtocolAsync(new DeleteCommandProtocol(namespace, ordered, writeConcern, deletes));
    }

    @Override
    public <T> T command(final String database, final BsonDocument command, final boolean slaveOk,
                         final FieldNameValidator fieldNameValidator,
                         final Decoder<T> commandResultDecoder) {
        return executeProtocol(new CommandProtocol<T>(database, command, slaveOk, fieldNameValidator, commandResultDecoder));
    }

    @Override
    public <T> MongoFuture<T> commandAsync(final String database, final BsonDocument command, final boolean slaveOk,
                                           final FieldNameValidator fieldNameValidator,
                                           final Decoder<T> commandResultDecoder) {
        return executeProtocolAsync(new CommandProtocol<T>(database, command, slaveOk, fieldNameValidator, commandResultDecoder));
    }

    @Override
    public <T> QueryResult<T> query(final MongoNamespace namespace, final BsonDocument queryDocument, final BsonDocument fields,
                                    final int numberToReturn, final int skip,
                                    final boolean slaveOk, final boolean tailableCursor,
                                    final boolean awaitData, final boolean noCursorTimeout,
                                    final boolean partial, final boolean oplogReplay,
                                    final Decoder<T> resultDecoder) {
        return executeProtocol(new QueryProtocol<T>(namespace, skip, numberToReturn, queryDocument, fields, resultDecoder)
                               .tailableCursor(tailableCursor)
                               .slaveOk(slaveOk)
                               .oplogReplay(oplogReplay)
                               .noCursorTimeout(noCursorTimeout)
                               .awaitData(awaitData)
                               .partial(partial));
    }

    @Override
    public <T> MongoFuture<QueryResult<T>> queryAsync(final MongoNamespace namespace, final BsonDocument queryDocument,
                                                      final BsonDocument fields, final int numberToReturn, final int skip,
                                                      final boolean slaveOk, final boolean tailableCursor,
                                                      final boolean awaitData, final boolean noCursorTimeout,
                                                      final boolean partial, final boolean oplogReplay,
                                                      final Decoder<T> resultDecoder) {
        return executeProtocolAsync(new QueryProtocol<T>(namespace, skip, numberToReturn, queryDocument, fields, resultDecoder)
                                    .tailableCursor(tailableCursor)
                                    .slaveOk(slaveOk)
                                    .oplogReplay(oplogReplay)
                                    .noCursorTimeout(noCursorTimeout)
                                    .awaitData(awaitData)
                                    .partial(partial));
    }

    @Override
    public <T> QueryResult<T> getMore(final MongoNamespace namespace, final long cursorId, final int numberToReturn,
                                      final Decoder<T> resultDecoder) {
        return executeProtocol(new GetMoreProtocol<T>(namespace, cursorId, numberToReturn, resultDecoder));
    }

    @Override
    public <T> MongoFuture<QueryResult<T>> getMoreAsync(final MongoNamespace namespace, final long cursorId, final int numberToReturn,
                                                        final Decoder<T> resultDecoder) {
        return executeProtocolAsync(new GetMoreProtocol<T>(namespace, cursorId, numberToReturn, resultDecoder));
    }

    @Override
    public void killCursor(final List<Long> cursors) {
        executeProtocol(new KillCursorProtocol(cursors));
    }

    @Override
    public MongoFuture<Void> killCursorAsync(final List<Long> cursors) {
        return executeProtocolAsync(new KillCursorProtocol(cursors));
    }

    private <T> T executeProtocol(final Protocol<T> protocol) {
        return protocolExecutor.execute(protocol, this.wrapped);
    }

    private <T> MongoFuture<T> executeProtocolAsync(final Protocol<T> protocol) {
        return protocolExecutor.executeAsync(protocol, this.wrapped);
    }
}
