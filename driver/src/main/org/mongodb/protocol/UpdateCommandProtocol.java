/*
 * Copyright (c) 2008 - 2014 MongoDB, Inc.
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

import org.mongodb.BulkWriteResult;
import org.mongodb.Document;
import org.mongodb.Encoder;
import org.mongodb.MongoNamespace;
import org.mongodb.WriteConcern;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.Connection;
import org.mongodb.connection.ServerDescription;
import org.mongodb.diagnostics.Loggers;
import org.mongodb.operation.UpdateRequest;
import org.mongodb.operation.WriteRequest;
import org.mongodb.protocol.message.UpdateCommandMessage;

import java.util.List;
import java.util.logging.Logger;

import static java.lang.String.format;
import static org.mongodb.assertions.Assertions.notNull;
import static org.mongodb.protocol.ProtocolHelper.getMessageSettings;

public class UpdateCommandProtocol extends WriteCommandProtocol {

    private static final Logger LOGGER = Loggers.getLogger("protocol.update");

    private final List<UpdateRequest> updates;
    private final Encoder<Document> queryEncoder;

    public UpdateCommandProtocol(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                                 final List<UpdateRequest> updates, final Encoder<Document> queryEncoder,
                                 final BufferProvider bufferProvider, final ServerDescription serverDescription,
                                 final Connection connection, final boolean closeConnection) {
        super(namespace, ordered, writeConcern, bufferProvider, serverDescription, connection, closeConnection);
        this.updates = notNull("update", updates);
        this.queryEncoder = notNull("queryEncoder", queryEncoder);
    }

    @Override
    public BulkWriteResult execute() {
        LOGGER.fine(format("Updating documents in namespace %s on connection [%s] to server %s", getNamespace(), getConnection().getId(),
                           getConnection().getServerAddress()));
        BulkWriteResult writeResult = super.execute();
        LOGGER.fine("Update completed");
        return writeResult;
    }

    @Override
    protected WriteRequest.Type getType() {
        return WriteRequest.Type.UPDATE;
    }

    @Override
    protected UpdateCommandMessage createRequestMessage() {
        return new UpdateCommandMessage(getNamespace(), isOrdered(), getWriteConcern(), updates,
                                        new CommandCodec<Document>(queryEncoder), getMessageSettings(getServerDescription()));
    }

    @Override
    protected Logger getLogger() {
        return LOGGER;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected List<WriteRequest> getRequests() {
        return (List) updates;
    }

}
