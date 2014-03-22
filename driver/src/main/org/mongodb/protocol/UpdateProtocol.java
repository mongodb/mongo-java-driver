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
import org.mongodb.connection.Connection;
import org.mongodb.connection.ServerDescription;
import org.mongodb.diagnostics.Loggers;
import org.mongodb.diagnostics.logging.Logger;
import org.mongodb.operation.UpdateRequest;
import org.mongodb.protocol.message.MessageSettings;
import org.mongodb.protocol.message.RequestMessage;
import org.mongodb.protocol.message.UpdateMessage;

import java.util.List;

import static java.lang.String.format;

public class UpdateProtocol extends WriteProtocol {
    private static final org.mongodb.diagnostics.logging.Logger LOGGER = Loggers.getLogger("protocol.update");

    private final List<UpdateRequest> updates;
    private final Encoder<Document> queryEncoder;

    public UpdateProtocol(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                          final List<UpdateRequest> updates, final Encoder<Document> queryEncoder,
                          final ServerDescription serverDescription, final Connection connection, final boolean closeConnection) {
        super(namespace, ordered, writeConcern, serverDescription, connection, closeConnection);
        this.updates = updates;
        this.queryEncoder = queryEncoder;
    }

    @Override
    public WriteResult execute() {
        LOGGER.debug(format("Updating documents in namespace %s on connection [%s] to server %s", getNamespace(), getConnection().getId(),
                            getConnection().getServerAddress()));
        WriteResult writeResult = super.execute();
        LOGGER.debug("Update completed");
        return writeResult;
    }

    @Override
    protected RequestMessage createRequestMessage(final MessageSettings settings) {
        return new UpdateMessage(getNamespace().getFullName(), updates, queryEncoder, settings);
    }

    @Override
    protected Logger getLogger() {
        return LOGGER;
    }

}
