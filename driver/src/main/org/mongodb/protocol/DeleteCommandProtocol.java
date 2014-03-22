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

import org.mongodb.BulkWriteResult;
import org.mongodb.Document;
import org.mongodb.Encoder;
import org.mongodb.MongoNamespace;
import org.mongodb.WriteConcern;
import org.mongodb.connection.Connection;
import org.mongodb.connection.ServerDescription;
import org.mongodb.diagnostics.Loggers;
import org.mongodb.diagnostics.logging.Logger;
import org.mongodb.operation.RemoveRequest;
import org.mongodb.operation.WriteRequest;
import org.mongodb.protocol.message.DeleteCommandMessage;

import java.util.List;

import static java.lang.String.format;
import static org.mongodb.assertions.Assertions.notNull;
import static org.mongodb.protocol.ProtocolHelper.getMessageSettings;

public class DeleteCommandProtocol extends WriteCommandProtocol {

    private static final Logger LOGGER = Loggers.getLogger("protocol.delete");

    private final List<RemoveRequest> removeRequests;
    private final Encoder<Document> queryEncoder;

    public DeleteCommandProtocol(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                                 final List<RemoveRequest> removeRequests, final Encoder<Document> queryEncoder,
                                 final ServerDescription serverDescription, final Connection connection, final boolean closeConnection) {
        super(namespace, ordered, writeConcern, serverDescription, connection, closeConnection);
        this.removeRequests = notNull("removes", removeRequests);
        this.queryEncoder = notNull("queryEncoder", queryEncoder);
    }

    @Override
    public BulkWriteResult execute() {
        LOGGER.debug(format("Deleting documents from namespace %s on connection [%s] to server %s", getNamespace(),
                            getConnection().getId(), getConnection().getServerAddress()));
        BulkWriteResult writeResult = super.execute();
        LOGGER.debug("Delete completed");
        return writeResult;
    }

    @Override
    protected WriteRequest.Type getType() {
        return WriteRequest.Type.REMOVE;
    }

    @Override
    protected DeleteCommandMessage createRequestMessage() {
        return new DeleteCommandMessage(getNamespace(), isOrdered(), getWriteConcern(), removeRequests, queryEncoder,
                                        getMessageSettings(getServerDescription()));
    }

    @Override
    protected org.mongodb.diagnostics.logging.Logger getLogger() {
        return LOGGER;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected List<WriteRequest> getRequests() {
        return ((List) removeRequests);
    }

}
