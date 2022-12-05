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

import com.mongodb.MongoNamespace;
import com.mongodb.ReadPreference;
import com.mongodb.RequestContext;
import com.mongodb.ServerApi;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.FieldNameValidator;
import org.bson.codecs.Decoder;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.connection.ProtocolHelper.getMessageSettings;

class CommandProtocolImpl<T> implements CommandProtocol<T> {
    private final MongoNamespace namespace;
    private final BsonDocument command;
    private final SplittablePayload payload;
    private final ReadPreference readPreference;
    private final FieldNameValidator commandFieldNameValidator;
    private final FieldNameValidator payloadFieldNameValidator;
    private final Decoder<T> commandResultDecoder;
    private final boolean responseExpected;
    private final ClusterConnectionMode clusterConnectionMode;
    private final RequestContext requestContext;
    private SessionContext sessionContext;
    private final ServerApi serverApi;

    CommandProtocolImpl(final String database, final BsonDocument command, final FieldNameValidator commandFieldNameValidator,
            @Nullable final ReadPreference readPreference, final Decoder<T> commandResultDecoder, final boolean responseExpected,
            @Nullable final SplittablePayload payload, @Nullable final FieldNameValidator payloadFieldNameValidator,
            final ClusterConnectionMode clusterConnectionMode, @Nullable final ServerApi serverApi, final RequestContext requestContext) {
        notNull("database", database);
        this.namespace = new MongoNamespace(notNull("database", database), MongoNamespace.COMMAND_COLLECTION_NAME);
        this.command = notNull("command", command);
        this.commandFieldNameValidator = notNull("commandFieldNameValidator", commandFieldNameValidator);
        this.readPreference = readPreference;
        this.commandResultDecoder = notNull("commandResultDecoder", commandResultDecoder);
        this.responseExpected = responseExpected;
        this.payload = payload;
        this.payloadFieldNameValidator = payloadFieldNameValidator;
        this.clusterConnectionMode = notNull("clusterConnectionMode", clusterConnectionMode);
        this.serverApi = serverApi;
        this.requestContext = notNull("requestContext", requestContext);

        isTrueArgument("payloadFieldNameValidator cannot be null if there is a payload.",
                payload == null || payloadFieldNameValidator != null);
    }

    @Nullable
    @Override
    public T execute(final InternalConnection connection) {
        return connection.sendAndReceive(getCommandMessage(connection), commandResultDecoder, sessionContext, requestContext);
    }

    @Override
    public void executeAsync(final InternalConnection connection, final SingleResultCallback<T> callback) {
        try {
            connection.sendAndReceiveAsync(getCommandMessage(connection), commandResultDecoder, sessionContext, requestContext,
                    (result, t) -> {
                        if (t != null) {
                            callback.onResult(null, t);
                        } else {
                            callback.onResult(result, null);
                        }
                    });
        } catch (Throwable t) {
            callback.onResult(null, t);
        }
    }

    @Override
    public CommandProtocolImpl<T> sessionContext(final SessionContext sessionContext) {
        this.sessionContext = sessionContext;
        return this;
    }

    private CommandMessage getCommandMessage(final InternalConnection connection) {
        return new CommandMessage(namespace, command, commandFieldNameValidator, readPreference,
                    getMessageSettings(connection.getDescription()), responseExpected, payload,
                payloadFieldNameValidator, clusterConnectionMode, serverApi);
    }
}
