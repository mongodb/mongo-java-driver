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
import com.mongodb.MongoServerException;
import com.mongodb.ServerApi;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonValue;
import org.bson.codecs.BsonDocumentCodec;

import java.util.Locale;

import static com.mongodb.MongoNamespace.COMMAND_COLLECTION_NAME;
import static com.mongodb.ReadPreference.primary;
import static com.mongodb.assertions.Assertions.assertNotNull;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class CommandHelper {

    static final String HELLO = "hello";
    static final String LEGACY_HELLO = "isMaster";
    static final String LEGACY_HELLO_LOWER = LEGACY_HELLO.toLowerCase(Locale.ROOT);

    static BsonDocument executeCommand(final String database, final BsonDocument command, final ClusterConnectionMode clusterConnectionMode,
            @Nullable final ServerApi serverApi, final InternalConnection internalConnection, final OperationContext operationContext) {
        return sendAndReceive(database, command, clusterConnectionMode, serverApi, internalConnection, operationContext);
    }

    static BsonDocument executeCommandWithoutCheckingForFailure(final String database, final BsonDocument command,
            final ClusterConnectionMode clusterConnectionMode, @Nullable final ServerApi serverApi,
            final InternalConnection internalConnection, final OperationContext operationContext) {
        try {
            return executeCommand(database, command, clusterConnectionMode, serverApi, internalConnection, operationContext);
        } catch (MongoServerException e) {
            return new BsonDocument();
        }
    }

    static void executeCommandAsync(final String database,
                                    final BsonDocument command,
                                    final ClusterConnectionMode clusterConnectionMode,
                                    @Nullable final ServerApi serverApi,
                                    final InternalConnection internalConnection,
                                    final OperationContext operationContext,
                                    final SingleResultCallback<BsonDocument> callback) {
        internalConnection.sendAndReceiveAsync(
                getCommandMessage(database, command, internalConnection, clusterConnectionMode, serverApi),
                new BsonDocumentCodec(), operationContext, (result, t) -> {
                    if (t != null) {
                        callback.onResult(null, t);
                    } else {
                        callback.onResult(result, null);
                    }
                });
    }

    static boolean isCommandOk(final BsonDocument response) {
        if (!response.containsKey("ok")) {
            return false;
        }
        BsonValue okValue = response.get("ok");
        if (okValue.isBoolean()) {
            return okValue.asBoolean().getValue();
        } else if (okValue.isNumber()) {
            return okValue.asNumber().intValue() == 1;
        } else {
            return false;
        }
    }

    private static BsonDocument sendAndReceive(final String database, final BsonDocument command,
                                               final ClusterConnectionMode clusterConnectionMode,
                                               @Nullable final ServerApi serverApi,
                                               final InternalConnection internalConnection,
                                               final OperationContext operationContext) {
            return assertNotNull(
                    internalConnection.sendAndReceive(
                            getCommandMessage(database, command, internalConnection, clusterConnectionMode, serverApi),
                            new BsonDocumentCodec(), operationContext)
            );
    }

    private static CommandMessage getCommandMessage(final String database, final BsonDocument command,
                                                    final InternalConnection internalConnection,
                                                    final ClusterConnectionMode clusterConnectionMode,
                                                    @Nullable final ServerApi serverApi) {
        return new CommandMessage(new MongoNamespace(database, COMMAND_COLLECTION_NAME), command, NoOpFieldNameValidator.INSTANCE, primary(),
                MessageSettings
                        .builder()
                         // Note: server version will be 0.0 at this point when called from InternalConnectionInitializer,
                         // which means OP_MSG will not be used
                        .maxWireVersion(internalConnection.getDescription().getMaxWireVersion())
                        .serverType(internalConnection.getDescription().getServerType())
                        .cryptd(internalConnection.getInitialServerDescription().isCryptd())
                        .build(),
                clusterConnectionMode, serverApi);
    }


    /**
     * Appends a user-defined maxTimeMS to the command if CSOT is not enabled.
     * This is necessary when maxTimeMS must be explicitly set on the command being explained,
     * rather than appending it lazily to the explain command in the {@link CommandMessage} via {@link TimeoutContext#setMaxTimeOverride(long)}.
     * This ensures backwards compatibility with pre-CSOT behavior.
     */
    public static void applyMaxTimeMS(final TimeoutContext timeoutContext, final BsonDocument command) {
        if (!timeoutContext.hasTimeoutMS()) {
            command.append("maxTimeMS", new BsonInt64(timeoutContext.getTimeoutSettings().getMaxTimeMS()));
        }
    }

    private CommandHelper() {
    }
}
