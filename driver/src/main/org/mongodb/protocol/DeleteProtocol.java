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
import org.mongodb.operation.RemoveRequest;
import org.mongodb.protocol.message.DeleteMessage;
import org.mongodb.protocol.message.MessageSettings;
import org.mongodb.protocol.message.RequestMessage;

import java.util.List;
import java.util.logging.Logger;

import static java.lang.String.format;

public class DeleteProtocol extends WriteProtocol {
    private static final Logger LOGGER = Loggers.getLogger("protocol.delete");

    private final List<RemoveRequest> deletes;
    private final Encoder<Document> queryEncoder;

    public DeleteProtocol(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                          final List<RemoveRequest> deletes,
                          final Encoder<Document> queryEncoder, final BufferProvider bufferProvider,
                          final ServerDescription serverDescription, final Connection connection, final boolean closeConnection) {
        super(namespace, bufferProvider, ordered, writeConcern, serverDescription, connection, closeConnection);
        this.deletes = deletes;
        this.queryEncoder = queryEncoder;
    }

    @Override
    public WriteResult execute() {
        LOGGER.fine(format("Deleting documents from namespace %s on connection [%s] to server %s", getNamespace(),
                           getConnection().getId(), getConnection().getServerAddress()));
        WriteResult writeResult = super.execute();
        LOGGER.fine("Delete completed");
        return writeResult;
    }

    @Override
    protected RequestMessage createRequestMessage(final MessageSettings settings) {
        return new DeleteMessage(getNamespace().getFullName(), deletes, queryEncoder, settings);
    }

    @Override
    protected Logger getLogger() {
        return LOGGER;
    }
}
