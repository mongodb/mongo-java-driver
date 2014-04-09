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
import org.mongodb.session.Session;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.mongodb.operation.DocumentHelper.putIfNotNull;
import static org.mongodb.operation.DocumentHelper.putIfNotZero;
import static org.mongodb.operation.OperationHelper.executeProtocol;

public class FindAndRemoveOperation<T> implements Operation<T> {
    private final MongoNamespace namespace;
    private final FindAndRemove<T> findAndRemove;
    private final CommandResultWithPayloadDecoder<T> resultDecoder;
    private final DocumentCodec commandEncoder = new DocumentCodec(PrimitiveCodecs.createDefault());

    public FindAndRemoveOperation(final MongoNamespace namespace, final FindAndRemove<T> findAndRemove, final Decoder<T> resultDecoder) {
        this.namespace = namespace;
        this.findAndRemove = findAndRemove;
        this.resultDecoder = new CommandResultWithPayloadDecoder<T>(resultDecoder, "value");
    }

    @SuppressWarnings("unchecked")
    @Override
    public T execute(final Session session) {
        CommandResult commandResult = executeProtocol(new CommandProtocol(namespace.getDatabaseName(), getFindAndRemoveDocument(),
                                                                          commandEncoder, resultDecoder),
                                                      session);
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
