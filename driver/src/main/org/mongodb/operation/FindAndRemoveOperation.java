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

import org.bson.codecs.Decoder;
import org.mongodb.Document;
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;
import org.mongodb.binding.AsyncWriteBinding;
import org.mongodb.binding.WriteBinding;
import org.mongodb.codecs.DocumentCodec;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static org.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync;
import static org.mongodb.operation.DocumentHelper.putIfNotNull;
import static org.mongodb.operation.DocumentHelper.putIfNotZero;

/**
 * An operation that atomically finds and removes a single document.
 *
 * @since 3.0
 */
public class FindAndRemoveOperation<T> implements AsyncWriteOperation<T>, WriteOperation<T> {
    private final MongoNamespace namespace;
    private final FindAndRemove<T> findAndRemove;
    private final CommandResultWithPayloadDecoder<T> resultDecoder;

    public FindAndRemoveOperation(final MongoNamespace namespace, final FindAndRemove<T> findAndRemove, final Decoder<T> resultDecoder) {
        this.namespace = namespace;
        this.findAndRemove = findAndRemove;
        this.resultDecoder = new CommandResultWithPayloadDecoder<T>(resultDecoder, "value");
    }

    @Override
    public T execute(final WriteBinding binding) {
        return executeWrappedCommandProtocol(namespace, getFindAndRemoveDocument(), new DocumentCodec(), resultDecoder, binding,
                                             FindAndModifyHelper.<T>transformer());
    }

    @Override
    public MongoFuture<T> executeAsync(final AsyncWriteBinding binding) {
        return executeWrappedCommandProtocolAsync(namespace, getFindAndRemoveDocument(), new DocumentCodec(), resultDecoder, binding,
                                                  FindAndModifyHelper.<T>transformer());
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
