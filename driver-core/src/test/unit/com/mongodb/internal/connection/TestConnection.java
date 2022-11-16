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

package com.mongodb.internal.connection;

import com.mongodb.ReadPreference;
import com.mongodb.RequestContext;
import com.mongodb.ServerApi;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.FieldNameValidator;
import org.bson.codecs.Decoder;

@SuppressWarnings({"rawtypes", "unchecked"})
class TestConnection implements Connection, AsyncConnection {
    private final InternalConnection internalConnection;
    private final ProtocolExecutor executor;
    private CommandProtocol enqueuedCommandProtocol;

    TestConnection(final InternalConnection internalConnection, final ProtocolExecutor executor) {
        this.internalConnection = internalConnection;
        this.executor = executor;
    }

    @Override
    public int getCount() {
        return 1;
    }

    @Override
    public TestConnection retain() {
        return this;
    }

    @Override
    public int release() {
        return 1;
    }

    @Override
    public ConnectionDescription getDescription() {
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    @Override
    public <T> T command(final String database, final BsonDocument command, final FieldNameValidator fieldNameValidator,
            final ReadPreference readPreference, final Decoder<T> commandResultDecoder, final SessionContext sessionContext,
            @Nullable final ServerApi serverApi, final RequestContext requestContext) {
        return executeEnqueuedCommandBasedProtocol(sessionContext);
    }

    @Override
    public <T> T command(final String database, final BsonDocument command, final FieldNameValidator commandFieldNameValidator,
            final ReadPreference readPreference, final Decoder<T> commandResultDecoder, final SessionContext sessionContext,
            @Nullable final ServerApi serverApi, final RequestContext requestContext, final boolean responseExpected,
            @Nullable final SplittablePayload payload, @Nullable final FieldNameValidator payloadFieldNameValidator) {
        return executeEnqueuedCommandBasedProtocol(sessionContext);
    }

    @Override
    public <T> void commandAsync(final String database, final BsonDocument command, final FieldNameValidator fieldNameValidator,
                                 final ReadPreference readPreference, final Decoder<T> commandResultDecoder,
                                 final SessionContext sessionContext, @Nullable final ServerApi serverApi,
                                 final RequestContext requestContext, final SingleResultCallback<T> callback) {
        executeEnqueuedCommandBasedProtocolAsync(sessionContext, callback);
    }

    @Override
    public <T> void commandAsync(final String database, final BsonDocument command, final FieldNameValidator commandFieldNameValidator,
                                 final ReadPreference readPreference, final Decoder<T> commandResultDecoder,
                                 final SessionContext sessionContext, final @Nullable ServerApi serverApi,
                                 final RequestContext requestContext, final boolean responseExpected,
                                 @Nullable final SplittablePayload payload, @Nullable final FieldNameValidator payloadFieldNameValidator,
                                 final SingleResultCallback<T> callback) {
        executeEnqueuedCommandBasedProtocolAsync(sessionContext, callback);
    }

    @Override
    public void markAsPinned(final PinningMode pinningMode) {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("unchecked")
    private <T> T executeEnqueuedCommandBasedProtocol(final SessionContext sessionContext) {
        return (T) executor.execute(enqueuedCommandProtocol, internalConnection, sessionContext);
    }

    @SuppressWarnings("unchecked")
    private <T> void executeEnqueuedCommandBasedProtocolAsync(final SessionContext sessionContext, final SingleResultCallback<T> callback) {
        executor.executeAsync(enqueuedCommandProtocol, internalConnection, sessionContext, callback);
    }

    void enqueueProtocol(final CommandProtocol protocol) {
        enqueuedCommandProtocol = protocol;
    }
}
