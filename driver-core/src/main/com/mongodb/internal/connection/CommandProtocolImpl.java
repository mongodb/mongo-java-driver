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
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.FieldNameValidator;
import org.bson.codecs.Decoder;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.connection.ProtocolHelper.getMessageSettings;

class CommandProtocolImpl<T> implements CommandProtocol<T> {
    private final String database;
    private final BsonDocument command;
    private final MessageSequences sequences;
    private final ReadPreference readPreference;
    private final FieldNameValidator commandFieldNameValidator;
    private final Decoder<T> commandResultDecoder;
    private final boolean responseExpected;
    private final ClusterConnectionMode clusterConnectionMode;
    private final OperationContext operationContext;

    CommandProtocolImpl(final String database, final BsonDocument command, final FieldNameValidator commandFieldNameValidator,
            @Nullable final ReadPreference readPreference, final Decoder<T> commandResultDecoder, final boolean responseExpected,
            final MessageSequences sequences, final ClusterConnectionMode clusterConnectionMode, final OperationContext operationContext) {
        notNull("database", database);
        this.database = notNull("database", database);
        this.command = notNull("command", command);
        this.commandFieldNameValidator = notNull("commandFieldNameValidator", commandFieldNameValidator);
        this.readPreference = readPreference;
        this.commandResultDecoder = notNull("commandResultDecoder", commandResultDecoder);
        this.responseExpected = responseExpected;
        this.sequences = sequences;
        this.clusterConnectionMode = notNull("clusterConnectionMode", clusterConnectionMode);
        this.operationContext = operationContext;
    }

    @Nullable
    @Override
    public T execute(final InternalConnection connection) {
        return connection.sendAndReceive(getCommandMessage(connection), commandResultDecoder, operationContext);
    }

    @Override
    public void executeAsync(final InternalConnection connection, final SingleResultCallback<T> callback) {
        try {
            connection.sendAndReceiveAsync(getCommandMessage(connection), commandResultDecoder, operationContext,
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
    public CommandProtocolImpl<T> withSessionContext(final SessionContext sessionContext) {
        return new CommandProtocolImpl<>(database, command, commandFieldNameValidator, readPreference,
                commandResultDecoder, responseExpected, sequences, clusterConnectionMode,
                operationContext.withSessionContext(sessionContext));
    }

    private CommandMessage getCommandMessage(final InternalConnection connection) {
        return new CommandMessage(database, command, commandFieldNameValidator, readPreference,
                    getMessageSettings(connection.getDescription(), connection.getInitialServerDescription()), responseExpected,
                sequences, clusterConnectionMode, operationContext.getServerApi());
    }
}
