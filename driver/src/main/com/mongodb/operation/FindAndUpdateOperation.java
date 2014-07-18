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

package com.mongodb.operation;

import com.mongodb.binding.AsyncWriteBinding;
import com.mongodb.binding.WriteBinding;
import com.mongodb.protocol.message.MappedFieldNameValidator;
import com.mongodb.protocol.message.NoOpFieldNameValidator;
import com.mongodb.protocol.message.UpdateFieldNameValidator;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.FieldNameValidator;
import org.bson.codecs.Decoder;
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;

import java.util.HashMap;
import java.util.Map;

import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync;
import static com.mongodb.operation.DocumentHelper.putIfNotNull;
import static com.mongodb.operation.DocumentHelper.putIfNotZero;
import static com.mongodb.operation.DocumentHelper.putIfTrue;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * An operation that atomically finds and updates a single document.
 *
 * @param <T> The document type
 * @since 3.0
 */
public class FindAndUpdateOperation<T> implements AsyncWriteOperation<T>, WriteOperation<T> {
    private final MongoNamespace namespace;
    private final FindAndUpdate findAndUpdate;
    private final Decoder<T> decoder;

    public FindAndUpdateOperation(final MongoNamespace namespace, final FindAndUpdate findAndUpdate, final Decoder<T> decoder) {
        this.namespace = namespace;
        this.findAndUpdate = findAndUpdate;
        this.decoder = decoder;
    }

    @Override
    public T execute(final WriteBinding binding) {
        return executeWrappedCommandProtocol(namespace, getCommand(), getValidator(),
                                             CommandResultDocumentCodec.create(decoder, "value"), binding,
                                             FindAndModifyHelper.<T>transformer());
    }

    @Override
    public MongoFuture<T> executeAsync(final AsyncWriteBinding binding) {
        return executeWrappedCommandProtocolAsync(namespace, getCommand(), getValidator(),
                                                  CommandResultDocumentCodec.create(decoder, "value"), binding,
                                                  FindAndModifyHelper.<T>transformer());
    }

    private BsonDocument getCommand() {
        BsonDocument command = new BsonDocument("findandmodify", new BsonString(namespace.getCollectionName()));
        putIfNotNull(command, "query", findAndUpdate.getFilter());
        putIfNotNull(command, "fields", findAndUpdate.getSelector());
        putIfNotNull(command, "sort", findAndUpdate.getSortCriteria());
        putIfTrue(command, "new", findAndUpdate.isReturnNew());
        putIfTrue(command, "upsert", findAndUpdate.isUpsert());
        putIfNotZero(command, "maxTimeMS", findAndUpdate.getOptions().getMaxTime(MILLISECONDS));

        command.put("update", findAndUpdate.getUpdateOperations());
        return command;
    }

    private FieldNameValidator getValidator() {
        Map<String, FieldNameValidator> map = new HashMap<String, FieldNameValidator>();
        map.put("update", new UpdateFieldNameValidator());

        return new MappedFieldNameValidator(new NoOpFieldNameValidator(), map);
    }
}
