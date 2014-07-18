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

package com.mongodb.protocol;

import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.connection.SingleResultCallback;
import com.mongodb.diagnostics.Loggers;
import com.mongodb.diagnostics.logging.Logger;
import org.bson.BsonDocument;
import org.bson.codecs.Decoder;
import org.mongodb.CommandResult;

class CommandResultCallback extends CommandResultBaseCallback {
    public static final Logger LOGGER = Loggers.getLogger("protocol.command");

    private final SingleResultCallback<CommandResult> callback;

    public CommandResultCallback(final SingleResultCallback<CommandResult> callback, final Decoder<BsonDocument> decoder,
                                 final long requestId, final ServerAddress serverAddress) {
        super(decoder, requestId, serverAddress);
        this.callback = callback;
    }

    @Override
    protected boolean callCallback(final CommandResult commandResult, final MongoException e) {
        if (e != null) {
            callback.onResult(null, e);
        } else {
            LOGGER.debug("Command execution completed with status " + commandResult.isOk());
            if (!commandResult.isOk()) {
                callback.onResult(null, ProtocolHelper.getCommandFailureException(commandResult));
            } else {
                callback.onResult(commandResult, null);
            }
        }
        return true;
    }
}
