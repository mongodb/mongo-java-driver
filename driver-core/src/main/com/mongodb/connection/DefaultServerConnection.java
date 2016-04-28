/*
 * Copyright (c) 2008-2015 MongoDB, Inc.
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
import com.mongodb.async.SingleResultCallback;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.DeleteRequest;
import com.mongodb.bulk.InsertRequest;
import com.mongodb.bulk.UpdateRequest;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import org.bson.BsonDocument;
import org.bson.FieldNameValidator;
import org.bson.codecs.Decoder;

import java.util.List;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.connection.ServerType.SHARD_ROUTER;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;

@SuppressWarnings("deprecation")  // because this class implements deprecated methods
class DefaultServerConnection extends AbstractReferenceCounted implements Connection, AsyncConnection {
    private static final Logger LOGGER = Loggers.getLogger("connection");
    private final InternalConnection wrapped;
    private final ProtocolExecutor protocolExecutor;
    private final ClusterConnectionMode clusterConnectionMode;

    public DefaultServerConnection(final InternalConnection wrapped, final ProtocolExecutor protocolExecutor,
                                   final ClusterConnectionMode clusterConnectionMode) {
        this.wrapped = wrapped;
        this.protocolExecutor = protocolExecutor;
        this.clusterConnectionMode = clusterConnectionMode;
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
    public void insertAsync(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                            final List<InsertRequest> inserts, final SingleResultCallback<WriteConcernResult> callback) {
        executeProtocolAsync(new InsertProtocol(namespace, ordered, writeConcern, inserts), callback);
    }

    @Override
    public WriteConcernResult update(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                                     final List<UpdateRequest> updates) {
        return executeProtocol(new UpdateProtocol(namespace, ordered, writeConcern, updates));
    }

    @Override
    public void updateAsync(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                            final List<UpdateRequest> updates, final SingleResultCallback<WriteConcernResult> callback) {
        executeProtocolAsync(new UpdateProtocol(namespace, ordered, writeConcern, updates), callback);
    }

    @Override
    public WriteConcernResult delete(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                                     final List<DeleteRequest> deletes) {
        return executeProtocol(new DeleteProtocol(namespace, ordered, writeConcern, deletes));
    }

    @Override
    public void deleteAsync(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                            final List<DeleteRequest> deletes, final SingleResultCallback<WriteConcernResult> callback) {
        executeProtocolAsync(new DeleteProtocol(namespace, ordered, writeConcern, deletes), callback);
    }

    @Override
    public BulkWriteResult insertCommand(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                                         final List<InsertRequest> inserts) {
        return insertCommand(namespace, ordered, writeConcern, null, inserts);
    }

    @Override
    public BulkWriteResult insertCommand(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                                         final Boolean bypassDocumentValidation, final List<InsertRequest> inserts) {
        return executeProtocol(new InsertCommandProtocol(namespace, ordered, writeConcern, bypassDocumentValidation, inserts));
    }

    @Override
    public void insertCommandAsync(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                                   final List<InsertRequest> inserts, final SingleResultCallback<BulkWriteResult> callback) {
        insertCommandAsync(namespace, ordered, writeConcern, null, inserts, callback);
    }

    @Override
    public void insertCommandAsync(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                                   final Boolean bypassDocumentValidation, final List<InsertRequest> inserts,
                                   final SingleResultCallback<BulkWriteResult> callback) {
        executeProtocolAsync(new InsertCommandProtocol(namespace, ordered, writeConcern, bypassDocumentValidation, inserts), callback);
    }

    @Override
    public BulkWriteResult updateCommand(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                                         final List<UpdateRequest> updates) {
        return updateCommand(namespace, ordered, writeConcern, null, updates);
    }

    @Override
    public BulkWriteResult updateCommand(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                                         final Boolean bypassDocumentValidation, final List<UpdateRequest> updates) {
        return executeProtocol(new UpdateCommandProtocol(namespace, ordered, writeConcern, bypassDocumentValidation, updates));
    }

    @Override
    public void updateCommandAsync(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                                   final List<UpdateRequest> updates, final SingleResultCallback<BulkWriteResult> callback) {
        updateCommandAsync(namespace, ordered, writeConcern, null, updates, callback);
    }

    @Override
    public void updateCommandAsync(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                                   final Boolean bypassDocumentValidation, final List<UpdateRequest> updates,
                                   final SingleResultCallback<BulkWriteResult> callback) {
        executeProtocolAsync(new UpdateCommandProtocol(namespace, ordered, writeConcern, bypassDocumentValidation, updates), callback);
    }

    @Override
    public BulkWriteResult deleteCommand(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                                         final List<DeleteRequest> deletes) {
        return executeProtocol(new DeleteCommandProtocol(namespace, ordered, writeConcern, deletes));

    }

    @Override
    public void deleteCommandAsync(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                                   final List<DeleteRequest> deletes, final SingleResultCallback<BulkWriteResult> callback) {
        executeProtocolAsync(new DeleteCommandProtocol(namespace, ordered, writeConcern, deletes), callback);
    }

    @Override
    public <T> T command(final String database, final BsonDocument command, final boolean slaveOk,
                         final FieldNameValidator fieldNameValidator,
                         final Decoder<T> commandResultDecoder) {
        return executeProtocol(new CommandProtocol<T>(database, command, fieldNameValidator, commandResultDecoder)
                               .slaveOk(getSlaveOk(slaveOk)));
    }

    @Override
    public <T> void commandAsync(final String database, final BsonDocument command, final boolean slaveOk,
                                           final FieldNameValidator fieldNameValidator,
                                           final Decoder<T> commandResultDecoder, final SingleResultCallback<T> callback) {
        executeProtocolAsync(new CommandProtocol<T>(database, command, fieldNameValidator, commandResultDecoder)
                             .slaveOk(getSlaveOk(slaveOk)),
                             callback);
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
                               .slaveOk(getSlaveOk(slaveOk))
                               .oplogReplay(oplogReplay)
                               .noCursorTimeout(noCursorTimeout)
                               .awaitData(awaitData)
                               .partial(partial));
    }

    @Override
    public <T> QueryResult<T> query(final MongoNamespace namespace, final BsonDocument queryDocument, final BsonDocument fields,
                                    final int skip, final int limit, final int batchSize,
                                    final boolean slaveOk, final boolean tailableCursor,
                                    final boolean awaitData, final boolean noCursorTimeout,
                                    final boolean partial, final boolean oplogReplay,
                                    final Decoder<T> resultDecoder) {
        return executeProtocol(new QueryProtocol<T>(namespace, skip, limit, batchSize, queryDocument, fields, resultDecoder)
                               .tailableCursor(tailableCursor)
                               .slaveOk(getSlaveOk(slaveOk))
                               .oplogReplay(oplogReplay)
                               .noCursorTimeout(noCursorTimeout)
                               .awaitData(awaitData)
                               .partial(partial));
    }

    @Override
    public <T> void queryAsync(final MongoNamespace namespace, final BsonDocument queryDocument, final BsonDocument fields,
                               final int numberToReturn, final int skip,
                               final boolean slaveOk, final boolean tailableCursor, final boolean awaitData, final boolean noCursorTimeout,
                               final boolean partial,
                               final boolean oplogReplay, final Decoder<T> resultDecoder,
                               final SingleResultCallback<QueryResult<T>> callback) {
        executeProtocolAsync(new QueryProtocol<T>(namespace, skip, numberToReturn, queryDocument, fields, resultDecoder)
                             .tailableCursor(tailableCursor)
                             .slaveOk(getSlaveOk(slaveOk))
                             .oplogReplay(oplogReplay)
                             .noCursorTimeout(noCursorTimeout)
                             .awaitData(awaitData)
                             .partial(partial), callback);
    }

    @Override
    public <T> void queryAsync(final MongoNamespace namespace, final BsonDocument queryDocument, final BsonDocument fields, final int skip,
                               final int limit, final int batchSize, final boolean slaveOk, final boolean tailableCursor,
                               final boolean awaitData, final boolean noCursorTimeout, final boolean partial, final boolean oplogReplay,
                               final Decoder<T> resultDecoder, final SingleResultCallback<QueryResult<T>> callback) {
        executeProtocolAsync(new QueryProtocol<T>(namespace, skip, limit, batchSize, queryDocument, fields, resultDecoder)
                             .tailableCursor(tailableCursor)
                             .slaveOk(getSlaveOk(slaveOk))
                             .oplogReplay(oplogReplay)
                             .noCursorTimeout(noCursorTimeout)
                             .awaitData(awaitData)
                             .partial(partial), callback);
    }

    @Override
    public <T> QueryResult<T> getMore(final MongoNamespace namespace, final long cursorId, final int numberToReturn,
                                      final Decoder<T> resultDecoder) {
        return executeProtocol(new GetMoreProtocol<T>(namespace, cursorId, numberToReturn, resultDecoder));
    }

    @Override
    public <T> void getMoreAsync(final MongoNamespace namespace, final long cursorId, final int numberToReturn,
                                 final Decoder<T> resultDecoder, final SingleResultCallback<QueryResult<T>> callback) {
        executeProtocolAsync(new GetMoreProtocol<T>(namespace, cursorId, numberToReturn, resultDecoder), callback);
    }

    @Override
    public void killCursor(final List<Long> cursors) {
        killCursor(null, cursors);
    }

    @Override
    public void killCursor(final MongoNamespace namespace, final List<Long> cursors) {
        executeProtocol(new KillCursorProtocol(namespace, cursors));
    }

    @Override
    public void killCursorAsync(final List<Long> cursors, final SingleResultCallback<Void> callback) {
        killCursorAsync(null, cursors, callback);
    }

    @Override
    public void killCursorAsync(final MongoNamespace namespace, final List<Long> cursors, final SingleResultCallback<Void> callback) {
        executeProtocolAsync(new KillCursorProtocol(namespace, cursors), callback);
    }

    private boolean getSlaveOk(final boolean slaveOk) {
        return slaveOk
               || (clusterConnectionMode == ClusterConnectionMode.SINGLE && wrapped.getDescription().getServerType() != SHARD_ROUTER);
    }

    private <T> T executeProtocol(final Protocol<T> protocol) {
        return protocolExecutor.execute(protocol, this.wrapped);
    }

    private <T> void executeProtocolAsync(final Protocol<T> protocol, final SingleResultCallback<T> callback) {
        SingleResultCallback<T> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
        try {
            protocolExecutor.executeAsync(protocol, this.wrapped, errHandlingCallback);
        } catch (Throwable t) {
            errHandlingCallback.onResult(null, t);
        }
    }
}
