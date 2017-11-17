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
import com.mongodb.MongoServerException;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.internal.connection.NoOpSessionContext;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import com.mongodb.session.SessionContext;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.codecs.BsonDocumentCodec;

import static com.mongodb.MongoNamespace.COMMAND_COLLECTION_NAME;
import static com.mongodb.ReadPreference.primary;

final class CommandHelper {
    static BsonDocument executeCommand(final String database, final BsonDocument command, final InternalConnection internalConnection) {
        return sendAndReceive(database, command, null, internalConnection);
    }

    static BsonDocument executeCommand(final String database, final BsonDocument command, final ClusterClock clusterClock,
                                       final InternalConnection internalConnection) {
        return sendAndReceive(database, command, clusterClock, internalConnection);
    }

    static BsonDocument executeCommandWithoutCheckingForFailure(final String database, final BsonDocument command,
                                                                final InternalConnection internalConnection) {
        try {
            return sendAndReceive(database, command, null, internalConnection);
        } catch (MongoServerException e) {
            return new BsonDocument();
        }
    }

    static void executeCommandAsync(final String database, final BsonDocument command, final InternalConnection internalConnection,
                                    final SingleResultCallback<BsonDocument> callback) {
        internalConnection.sendAndReceiveAsync(getCommandMessage(database, command, internalConnection), new BsonDocumentCodec(),
                NoOpSessionContext.INSTANCE, new SingleResultCallback<BsonDocument>() {
                    @Override
                    public void onResult(final BsonDocument result, final Throwable t) {
                        if (t != null) {
                            callback.onResult(null, t);
                        } else {
                            callback.onResult(result, null);
                        }
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
                                               final ClusterClock clusterClock, final InternalConnection internalConnection) {
        SessionContext sessionContext = clusterClock == null ? NoOpSessionContext.INSTANCE
                : new ClusterClockAdvancingSessionContext(NoOpSessionContext.INSTANCE, clusterClock);
        return internalConnection.sendAndReceive(getCommandMessage(database, command, internalConnection), new BsonDocumentCodec(),
                sessionContext);
    }

    private static CommandMessage getCommandMessage(final String database, final BsonDocument command,
                                                    final InternalConnection internalConnection) {
        return new CommandMessage(new MongoNamespace(database, COMMAND_COLLECTION_NAME), command, new NoOpFieldNameValidator(), primary(),
                MessageSettings
                        .builder()
                         // Note: server version will be 0.0 at this point when called from InternalConnectionInitializer,
                         // which means OP_MSG will not be used
                        .serverVersion(internalConnection.getDescription().getServerVersion())
                        .build()
        );
    }

    private CommandHelper() {
    }
}
