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
import org.mongodb.MongoNamespace;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.codecs.PrimitiveCodecs;
import org.mongodb.protocol.CommandProtocol;
import org.mongodb.session.ServerConnectionProvider;
import org.mongodb.session.Session;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.mongodb.operation.DocumentHelper.putIfNotNull;
import static org.mongodb.operation.DocumentHelper.putIfNotZero;

public class FindAndRemoveOperation<T> extends BaseOperation<T> {
    private final MongoNamespace namespace;
    private final FindAndRemove<T> findAndRemove;
    private final CommandResultWithPayloadDecoder<T> resultDecoder;
    private final DocumentCodec commandEncoder = new DocumentCodec(PrimitiveCodecs.createDefault());

    public FindAndRemoveOperation(final MongoNamespace namespace, final FindAndRemove<T> findAndRemove, final Decoder<T> resultDecoder,
                                  final Session session, final boolean closeSession) {
        super(session, closeSession);
        this.namespace = namespace;
        this.findAndRemove = findAndRemove;
        this.resultDecoder = new CommandResultWithPayloadDecoder<T>(resultDecoder);
    }

    @SuppressWarnings("unchecked")
    @Override
    public T execute() {
        ServerConnectionProvider provider = getPrimaryServerConnectionProvider();
        CommandResult commandResult = new CommandProtocol(namespace.getDatabaseName(), getFindAndRemoveDocument(),
                                                          commandEncoder, resultDecoder,
                                                          provider.getServerDescription(), provider.getConnection(), true)
                                          .execute();
        return (T) commandResult.getResponse().get("value");
    }

    private Document getFindAndRemoveDocument() {
        Document command = new Document("findandmodify", namespace.getCollectionName());
        putIfNotNull(command, "query", findAndRemove.getFilter());
        putIfNotNull(command, "fields", findAndRemove.getSelector());
        putIfNotNull(command, "sort", findAndRemove.getSortCriteria());
        putIfNotZero(command, "maxTimeMS", findAndRemove.getOptions().getMaxTime(MILLISECONDS));

        command.put("remove", true);
        return command;
    }
}
