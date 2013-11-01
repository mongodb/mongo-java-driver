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

import org.mongodb.MongoNamespace;
import org.mongodb.WriteConcern;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.Connection;
import org.mongodb.connection.ServerDescription;
import org.mongodb.protocol.message.MessageSettings;
import org.mongodb.protocol.message.RequestMessage;

import java.util.logging.Logger;

class GenericWriteProtocol extends WriteProtocol {
    private final RequestMessage requestMessage;

    public GenericWriteProtocol(final MongoNamespace namespace, final BufferProvider bufferProvider,
                                final RequestMessage requestMessage, final boolean ordered, final WriteConcern writeConcern,
                                final ServerDescription serverDescription, final Connection connection, final boolean closeConnection) {
        super(namespace, bufferProvider, ordered, writeConcern, serverDescription, connection, closeConnection);
        this.requestMessage = requestMessage;
    }

    @Override
    protected RequestMessage createRequestMessage(final MessageSettings settings) {
        return requestMessage;
    }

    @Override
    protected Logger getLogger() {
        throw new UnsupportedOperationException();
    }
}
