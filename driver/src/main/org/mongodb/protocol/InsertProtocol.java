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

import org.mongodb.Encoder;
import org.mongodb.MongoNamespace;
import org.mongodb.WriteConcern;
import org.mongodb.WriteResult;
import org.mongodb.connection.Connection;
import org.mongodb.connection.ServerDescription;
import org.mongodb.diagnostics.Loggers;
import org.mongodb.diagnostics.logging.Logger;
import org.mongodb.operation.InsertRequest;
import org.mongodb.protocol.message.InsertMessage;
import org.mongodb.protocol.message.MessageSettings;
import org.mongodb.protocol.message.RequestMessage;

import java.util.List;

import static java.lang.String.format;

public class InsertProtocol<T> extends WriteProtocol {

    private static final Logger LOGGER = Loggers.getLogger("protocol.insert");

    private final List<InsertRequest<T>> insertRequestList;
    private final Encoder<T> encoder;

    public InsertProtocol(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                          final List<InsertRequest<T>> insertRequestList, final Encoder<T> encoder) {
        super(namespace, ordered, writeConcern);
        this.insertRequestList = insertRequestList;
        this.encoder = encoder;
    }

    @Override
    public WriteResult execute(final Connection connection, final ServerDescription serverDescription) {
        LOGGER.debug(format("Inserting %d documents into namespace %s on connection [%s] to server %s", insertRequestList.size(),
                            getNamespace(), connection.getId(), connection.getServerAddress()));
        WriteResult writeResult = super.execute(connection, serverDescription);
        LOGGER.debug("Insert completed");
        return writeResult;
    }

    protected RequestMessage createRequestMessage(final MessageSettings settings) {
        return new InsertMessage<T>(getNamespace().getFullName(), isOrdered(), getWriteConcern(), insertRequestList, encoder, settings);
    }

    @Override
    protected org.mongodb.diagnostics.logging.Logger getLogger() {
        return LOGGER;
    }
}
