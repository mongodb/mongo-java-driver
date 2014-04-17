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
import org.mongodb.Function;
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.codecs.PrimitiveCodecs;
import org.mongodb.session.Session;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.mongodb.operation.DocumentHelper.putIfNotNull;
import static org.mongodb.operation.DocumentHelper.putIfNotZero;
import static org.mongodb.operation.DocumentHelper.putIfTrue;
import static org.mongodb.operation.OperationHelper.executeWrappedCommandProtocol;
import static org.mongodb.operation.OperationHelper.executeWrappedCommandProtocolAsync;
import static org.mongodb.operation.OperationHelper.transformResult;

public class FindAndUpdateOperation<T> implements AsyncOperation<T>, Operation<T> {
    private final MongoNamespace namespace;
    private final FindAndUpdate<T> findAndUpdate;
    private final CommandResultWithPayloadDecoder<T> resultDecoder;
    private final DocumentCodec commandEncoder = new DocumentCodec(PrimitiveCodecs.createDefault());

    public FindAndUpdateOperation(final MongoNamespace namespace, final FindAndUpdate<T> findAndUpdate, final Decoder<T> resultDecoder) {
        this.namespace = namespace;
        this.findAndUpdate = findAndUpdate;
        this.resultDecoder = new CommandResultWithPayloadDecoder<T>(resultDecoder, "value");
    }

    @Override
    public T execute(final Session session) {
        validateUpdateDocumentToEnsureItHasUpdateOperators(findAndUpdate.getUpdateOperations());
        CommandResult result = executeWrappedCommandProtocol(namespace, createFindAndUpdateDocument(), commandEncoder, resultDecoder,
                                                             session);
        return transformResult(result, transformer());
    }

    @Override
    public MongoFuture<T> executeAsync(final Session session) {
        validateUpdateDocumentToEnsureItHasUpdateOperators(findAndUpdate.getUpdateOperations());
        MongoFuture<CommandResult> result = executeWrappedCommandProtocolAsync(namespace,
                                                                               createFindAndUpdateDocument(),
                                                                               commandEncoder,
                                                                               resultDecoder,
                                                                               session);
        return transformResult(result, transformer());
    }

    private Function<CommandResult, T> transformer() {
        return new Function<CommandResult, T>() {
            @SuppressWarnings("unchecked")
            @Override
            public T apply(final CommandResult result) {
                return (T) result.getResponse().get("value");
            }
        };
    }

    private void validateUpdateDocumentToEnsureItHasUpdateOperators(final Document value) {
        for (final String field : value.keySet()) {
            if (field.startsWith("$")) {
                return;
            }
        }
        throw new IllegalArgumentException(format("Find and update requires an update operator (beginning with '$') in the update "
                                                  + "Document: %s", value));
    }

    private Document createFindAndUpdateDocument() {
        Document command = new Document("findandmodify", namespace.getCollectionName());
        putIfNotNull(command, "query", findAndUpdate.getFilter());
        putIfNotNull(command, "fields", findAndUpdate.getSelector());
        putIfNotNull(command, "sort", findAndUpdate.getSortCriteria());
        putIfTrue(command, "new", findAndUpdate.isReturnNew());
        putIfTrue(command, "upsert", findAndUpdate.isUpsert());
        putIfNotZero(command, "maxTimeMS", findAndUpdate.getOptions().getMaxTime(MILLISECONDS));

        command.put("update", findAndUpdate.getUpdateOperations());
        return command;
    }

}
