/*
 * Copyright 2008-present MongoDB, Inc.
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
package com.mongodb.client.syncadapter;

import com.mongodb.MongoNamespace;
import com.mongodb.ReadPreference;
import com.mongodb.RequestContext;
import com.mongodb.ServerApi;
import com.mongodb.WriteConcernResult;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.internal.bulk.DeleteRequest;
import com.mongodb.internal.bulk.InsertRequest;
import com.mongodb.internal.bulk.UpdateRequest;
import com.mongodb.internal.connection.AsyncConnection;
import com.mongodb.internal.connection.Connection;
import com.mongodb.internal.connection.QueryResult;
import com.mongodb.internal.connection.SplittablePayload;
import com.mongodb.internal.session.SessionContext;
import org.bson.BsonDocument;
import org.bson.FieldNameValidator;
import org.bson.codecs.Decoder;

import java.util.List;

public final class SyncConnection implements Connection {
    private final AsyncConnection wrapped;

    public SyncConnection(final AsyncConnection connection) {
        wrapped = connection;
    }

    @Override
    public int getCount() {
        return wrapped.getCount();
    }

    @Override
    public void release() {
        wrapped.release();
    }

    @Override
    public Connection retain() {
        wrapped.retain();
        return this;
    }

    @Override
    public ConnectionDescription getDescription() {
        return wrapped.getDescription();
    }

    @Override
    public WriteConcernResult insert(final MongoNamespace namespace, final boolean ordered, final InsertRequest insertRequest,
            final RequestContext requestContext) {
        SupplyingCallback<WriteConcernResult> callback = new SupplyingCallback<>();
        wrapped.insertAsync(namespace, ordered, insertRequest, requestContext, callback);
        return callback.get();
    }

    @Override
    public WriteConcernResult update(final MongoNamespace namespace, final boolean ordered, final UpdateRequest updateRequest,
            final RequestContext requestContext) {
        SupplyingCallback<WriteConcernResult> callback = new SupplyingCallback<>();
        wrapped.updateAsync(namespace, ordered, updateRequest, requestContext, callback);
        return callback.get();
    }

    @Override
    public WriteConcernResult delete(final MongoNamespace namespace, final boolean ordered, final DeleteRequest deleteRequest,
            final RequestContext requestContext) {
        SupplyingCallback<WriteConcernResult> callback = new SupplyingCallback<>();
        wrapped.deleteAsync(namespace, ordered, deleteRequest, requestContext, callback);
        return callback.get();
    }

    @Override
    public <T> T command(final String database, final BsonDocument command, final FieldNameValidator fieldNameValidator,
            final ReadPreference readPreference, final Decoder<T> commandResultDecoder, final SessionContext sessionContext,
            final ServerApi serverApi, final RequestContext requestContext) {
        SupplyingCallback<T> callback = new SupplyingCallback<>();
        wrapped.commandAsync(database, command, fieldNameValidator, readPreference, commandResultDecoder, sessionContext, serverApi,
                requestContext, callback);
        return callback.get();
    }

    @Override
    public <T> T command(final String database, final BsonDocument command, final FieldNameValidator commandFieldNameValidator,
            final ReadPreference readPreference, final Decoder<T> commandResultDecoder, final SessionContext sessionContext,
            final ServerApi serverApi, final RequestContext requestContext, final boolean responseExpected, final SplittablePayload payload,
            final FieldNameValidator payloadFieldNameValidator) {
        SupplyingCallback<T> callback = new SupplyingCallback<>();
        wrapped.commandAsync(database, command, commandFieldNameValidator, readPreference, commandResultDecoder, sessionContext,
                serverApi, requestContext, responseExpected, payload, payloadFieldNameValidator, callback);
        return callback.get();
    }

    @Override
    public <T> QueryResult<T> query(final MongoNamespace namespace, final BsonDocument queryDocument, final BsonDocument fields,
            final int skip, final int limit, final int batchSize, final boolean secondaryOk, final boolean tailableCursor,
            final boolean awaitData, final boolean noCursorTimeout, final boolean partial, final boolean oplogReplay,
            final Decoder<T> resultDecoder, final RequestContext requestContext) {
        SupplyingCallback<QueryResult<T>> callback = new SupplyingCallback<>();
        wrapped.queryAsync(namespace, queryDocument, fields, skip, limit, batchSize, secondaryOk, tailableCursor, awaitData,
                noCursorTimeout, partial, oplogReplay, resultDecoder, requestContext, callback);
        return callback.get();
    }

    @Override
    public <T> QueryResult<T> getMore(final MongoNamespace namespace, final long cursorId, final int numberToReturn,
            final Decoder<T> resultDecoder, final RequestContext requestContext) {
        SupplyingCallback<QueryResult<T>> callback = new SupplyingCallback<>();
        wrapped.getMoreAsync(namespace, cursorId, numberToReturn, resultDecoder, requestContext, callback);
        return callback.get();
    }

    @Override
    public void killCursor(final MongoNamespace namespace, final List<Long> cursors, final RequestContext requestContext) {
        SupplyingCallback<Void> callback = new SupplyingCallback<>();
        wrapped.killCursorAsync(namespace, cursors, requestContext, callback);
        callback.get();
    }

    @Override
    public void markAsPinned(final PinningMode pinningMode) {
        wrapped.markAsPinned(pinningMode);
    }
}
