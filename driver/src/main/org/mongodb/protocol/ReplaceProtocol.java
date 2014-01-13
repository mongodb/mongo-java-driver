/*
 * Copyright (c) 2008 MongoDB, Inc.
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

package org.mongodb.protocol;

import org.mongodb.Document;
import org.mongodb.Encoder;
import org.mongodb.MongoNamespace;
import org.mongodb.WriteConcern;
import org.mongodb.WriteResult;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.Connection;
import org.mongodb.connection.ServerDescription;
import org.mongodb.diagnostics.Loggers;
import org.mongodb.operation.Replace;
import org.mongodb.protocol.message.MessageSettings;
import org.mongodb.protocol.message.ReplaceMessage;
import org.mongodb.protocol.message.RequestMessage;

import java.util.List;
import java.util.logging.Logger;

import static java.lang.String.format;

public class ReplaceProtocol<T> extends WriteProtocol {
    private static final Logger LOGGER = Loggers.getLogger("protocol.replace");

    private final List<Replace<T>> replaces;
    private final Encoder<Document> queryEncoder;
    private final Encoder<T> encoder;

    public ReplaceProtocol(final MongoNamespace namespace, final WriteConcern writeConcern, final List<Replace<T>> replaces,
                           final Encoder<Document> queryEncoder, final Encoder<T> encoder, final BufferProvider bufferProvider,
                           final ServerDescription serverDescription, final Connection connection, final boolean closeConnection) {
        super(namespace, bufferProvider, writeConcern, serverDescription, connection, closeConnection);
        this.replaces = replaces;
        this.queryEncoder = queryEncoder;
        this.encoder = encoder;
    }

    @Override
    public WriteResult execute() {
        LOGGER.fine(format("Replacing document in namespace %s on connection [%s] to server %s", getNamespace(), getConnection().getId(),
                           getConnection().getServerAddress()));
        WriteResult writeResult = super.execute();
        LOGGER.fine("Replace completed");
        return writeResult;
    }

    @Override
    protected RequestMessage createRequestMessage(final MessageSettings settings) {
        return new ReplaceMessage<T>(getNamespace().getFullName(), replaces, queryEncoder, encoder, settings);
    }

    @Override
    protected Logger getLogger() {
        return LOGGER;
    }
}
