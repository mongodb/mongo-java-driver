/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb.operation.protocol;

import org.mongodb.Document;
import org.mongodb.Encoder;
import org.mongodb.MongoNamespace;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.Connection;
import org.mongodb.connection.ServerDescription;
import org.mongodb.operation.Update;

public class UpdateProtocolOperation extends WriteProtocolOperation {
    private final Update update;
    private final Encoder<Document> queryEncoder;

    public UpdateProtocolOperation(final MongoNamespace namespace, final Update update, final Encoder<Document> queryEncoder,
                                   final BufferProvider bufferProvider, final ServerDescription serverDescription,
                                   final Connection connection, final boolean closeConnection) {
        super(namespace, bufferProvider, update.getWriteConcern(), serverDescription, connection, closeConnection);
        this.update = update;
        this.queryEncoder = queryEncoder;
    }

    @Override
    protected RequestMessage createRequestMessage(final MessageSettings settings) {
        return new UpdateMessage(getNamespace().getFullName(), update, queryEncoder, settings);
    }

    @Override
    public Update getWrite() {
        return update;
    }}
