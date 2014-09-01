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

import com.mongodb.Function;
import com.mongodb.MongoNamespace;
import com.mongodb.async.MongoFuture;
import com.mongodb.binding.AsyncReadBinding;
import com.mongodb.binding.ReadBinding;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.codecs.BsonDocumentCodec;
import org.mongodb.CommandResult;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync;
import static com.mongodb.operation.DocumentHelper.putIfNotNull;
import static com.mongodb.operation.DocumentHelper.putIfNotZero;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * An operation that executes a count.
 *
 * @since 3.0
 */
public class CountOperation implements AsyncReadOperation<Long>, ReadOperation<Long> {
    private final MongoNamespace namespace;
    private final Find find;

    public CountOperation(final MongoNamespace namespace, final Find find) {
        this.namespace = notNull("namespace", namespace);
        this.find = notNull("find", find);
    }


    public Long execute(final ReadBinding binding) {
        return executeWrappedCommandProtocol(namespace, asCommandDocument(), new BsonDocumentCodec(), binding,
                                             transformer());
    }

    @Override
    public MongoFuture<Long> executeAsync(final AsyncReadBinding binding) {
        return executeWrappedCommandProtocolAsync(namespace, asCommandDocument(), new BsonDocumentCodec(),
                                                  binding, transformer());
    }

    private Function<CommandResult, Long> transformer() {
        return new Function<CommandResult, Long>() {
            @Override
            public Long apply(final CommandResult result) {
                return (result.getResponse().getNumber("n")).longValue();
            }
        };
    }

    private BsonDocument asCommandDocument() {
        BsonDocument document = new BsonDocument("count", new BsonString(namespace.getCollectionName()));
        putIfNotNull(document, "query", find.getFilter());
        putIfNotZero(document, "limit", find.getLimit());
        putIfNotZero(document, "skip", find.getSkip());
        putIfNotNull(document, "hint", find.getHint());
        putIfNotZero(document, "maxTimeMS", find.getOptions().getMaxTime(MILLISECONDS));
        return document;
    }
}
