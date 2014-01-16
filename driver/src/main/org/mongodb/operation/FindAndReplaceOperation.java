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

package org.mongodb.operation;

import org.mongodb.CommandResult;
import org.mongodb.Decoder;
import org.mongodb.Document;
import org.mongodb.Encoder;
import org.mongodb.MongoNamespace;
import org.mongodb.connection.BufferProvider;
import org.mongodb.protocol.CommandProtocol;
import org.mongodb.session.ServerConnectionProvider;
import org.mongodb.session.Session;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.mongodb.operation.DocumentHelper.putIfNotNull;
import static org.mongodb.operation.DocumentHelper.putIfNotZero;
import static org.mongodb.operation.DocumentHelper.putIfTrue;

public class FindAndReplaceOperation<T> extends BaseOperation<T> {
    private final MongoNamespace namespace;
    private final FindAndReplace<T> findAndReplace;
    private final CommandResultWithPayloadDecoder<T> resultDecoder;
    private final CommandWithPayloadEncoder<T> commandEncoder;

    public FindAndReplaceOperation(final MongoNamespace namespace, final FindAndReplace<T> findAndReplace, final Decoder<T> payloadDecoder,
                                   final Encoder<T> payloadEncoder, final BufferProvider bufferProvider, final Session session,
                                   final boolean closeSession) {
        super(bufferProvider, session, closeSession);
        this.namespace = namespace;
        this.findAndReplace = findAndReplace;
        resultDecoder = new CommandResultWithPayloadDecoder<T>(payloadDecoder);
        commandEncoder = new CommandWithPayloadEncoder<T>("update", payloadEncoder);
    }

    @SuppressWarnings("unchecked")
    @Override
    public T execute() {
        ServerConnectionProvider provider = getPrimaryServerConnectionProvider();
        CommandResult commandResult = new CommandProtocol(namespace.getDatabaseName(), createFindAndReplaceDocument(),
                                                          commandEncoder, resultDecoder, getBufferProvider(),
                                                          provider.getServerDescription(), provider.getConnection(), true)
                                          .execute();
        return (T) commandResult.getResponse().get("value");
    }

    private Document createFindAndReplaceDocument() {
        Document command = new Document("findandmodify", namespace.getCollectionName());
        putIfNotNull(command, "query", findAndReplace.getFilter());
        putIfNotNull(command, "fields", findAndReplace.getSelector());
        putIfNotNull(command, "sort", findAndReplace.getSortCriteria());
        putIfTrue(command, "new", findAndReplace.isReturnNew());
        putIfTrue(command, "upsert", findAndReplace.isUpsert());
        putIfNotZero(command, "maxTimeMS", findAndReplace.getOptions().getMaxTime(MILLISECONDS));

        command.put("update", findAndReplace.getReplacement());
        return command;
    }
}
