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
import org.mongodb.Encoder;
import org.mongodb.MongoNamespace;
import org.mongodb.WriteConcern;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.connection.Connection;
import org.mongodb.connection.ServerDescription;
import org.mongodb.diagnostics.Loggers;
import org.mongodb.diagnostics.logging.Logger;
import org.mongodb.operation.InsertRequest;
import org.mongodb.operation.WriteRequest;
import org.mongodb.protocol.message.InsertCommandMessage;

import java.util.List;

import static java.lang.String.format;
import static org.mongodb.protocol.ProtocolHelper.getMessageSettings;

public class InsertCommandProtocol<T> extends WriteCommandProtocol {

    private static final Logger LOGGER = Loggers.getLogger("protocol.insert");

    private final List<InsertRequest<T>> insertRequests;
    private final Encoder<T> encoder;

    public InsertCommandProtocol(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                                 final List<InsertRequest<T>> insertRequests, final Encoder<T> encoder,
                                 final ServerDescription serverDescription, final Connection connection, final boolean closeConnection) {
        super(namespace, ordered, writeConcern, serverDescription, connection, closeConnection);
        this.insertRequests = insertRequests;
        this.encoder = encoder;
    }

    @Override
    public BulkWriteResult execute() {
        LOGGER.debug(format("Inserting %d documents into namespace %s on connection [%s] to server %s", insertRequests.size(),
                            getNamespace(), getConnection().getId(), getConnection().getServerAddress()));
        BulkWriteResult writeResult = super.execute();
        LOGGER.debug("Insert completed");
        return writeResult;
    }

    @Override
    protected WriteRequest.Type getType() {
        return WriteRequest.Type.INSERT;
    }

    @Override
    protected InsertCommandMessage<T> createRequestMessage() {
        return new InsertCommandMessage<T>(getNamespace(), isOrdered(), getWriteConcern(), insertRequests, new DocumentCodec(),
                                           encoder, getMessageSettings(getServerDescription()));
    }

    @Override
    protected org.mongodb.diagnostics.logging.Logger getLogger() {
        return LOGGER;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected List<WriteRequest> getRequests() {
        return (List) insertRequests;
    }

}
