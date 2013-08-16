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
import org.mongodb.WriteConcern;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.Connection;
import org.mongodb.connection.ServerDescription;
import org.mongodb.operation.Replace;

import java.util.List;

import static org.mongodb.assertions.Assertions.notNull;
import static org.mongodb.operation.OperationHelpers.getMessageSettings;

public class ReplaceCommandProtocol<T> extends WriteCommandProtocol {
    private final List<Replace<T>> replaces;
    private final Encoder<Document> queryEncoder;
    private final Encoder<T> encoder;

    public ReplaceCommandProtocol(final MongoNamespace namespace, final WriteConcern writeConcern, final List<Replace<T>> replaces,
                                  final Encoder<Document> queryEncoder, final Encoder<T> encoder, final BufferProvider bufferProvider,
                                  final ServerDescription serverDescription, final Connection connection, final boolean closeConnection) {
        super(namespace, writeConcern, bufferProvider, serverDescription, connection, closeConnection);
        this.replaces = notNull("replaces", replaces);
        this.queryEncoder = notNull("queryEncoder", queryEncoder);
        this.encoder = notNull("encoder", encoder);
    }

    @Override
    protected RequestMessage createRequestMessage() {
        return new ReplaceCommandMessage<T>(getNamespace(), getWriteConcern(), replaces, queryEncoder, encoder,
                getMessageSettings(getServerDescription()));
    }
}
