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

package org.mongodb.operation;

import org.mongodb.CommandResult;
import org.mongodb.Decoder;
import org.mongodb.Document;
import org.mongodb.Encoder;
import org.mongodb.MongoFuture;
import org.mongodb.ReadPreference;
import org.mongodb.session.Session;

import static org.mongodb.operation.CommandReadPreferenceHelper.isQuery;
import static org.mongodb.operation.OperationHelper.executeWrappedCommandProtocol;
import static org.mongodb.operation.OperationHelper.executeWrappedCommandProtocolAsync;

/**
 * An operation that executes a command.
 *
 * @since 3.0
 */
public class CommandOperation implements AsyncOperation<CommandResult>, Operation<CommandResult> {
    private final Encoder<Document> commandEncoder;
    private final Decoder<Document> commandDecoder;
    private final String database;
    private final Document commandDocument;
    private final ReadPreference readPreference;

    public CommandOperation(final String database, final Document command, final ReadPreference readPreference,
                            final Decoder<Document> commandDecoder, final Encoder<Document> commandEncoder) {
        this.database = database;
        this.commandEncoder = commandEncoder;
        this.commandDecoder = commandDecoder;
        this.commandDocument = command;
        this.readPreference = readPreference;
    }

    @Override
    public CommandResult execute(final Session session) {
        if (isQuery(commandDocument)) {
            return executeWrappedCommandProtocol(database, commandDocument, commandEncoder, commandDecoder, readPreference, session);
        } else {
            return executeWrappedCommandProtocol(database, commandDocument, commandEncoder, commandDecoder, session);
        }
    }

    @Override
    public MongoFuture<CommandResult> executeAsync(final Session session) {
        if (isQuery(commandDocument)) {
            return executeWrappedCommandProtocolAsync(database, commandDocument, commandEncoder, commandDecoder, readPreference, session);
        } else {
            return executeWrappedCommandProtocolAsync(database, commandDocument, commandEncoder, commandDecoder, session);
        }
    }
}
