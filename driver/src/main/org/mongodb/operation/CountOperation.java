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
import org.mongodb.Document;
import org.mongodb.Encoder;
import org.mongodb.Function;
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;
import org.mongodb.binding.AsyncReadBinding;
import org.mongodb.binding.ReadBinding;
import org.mongodb.codecs.DocumentCodec;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static org.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync;

/**
 * An operation that executes a count.
 *
 * @since 3.0
 */
public class CountOperation implements ReadOperation<Long>, AsyncReadOperation<Long> {
    private final Encoder<Document> encoder;
    private final MongoNamespace namespace;
    private final Find find;

    public CountOperation(final MongoNamespace namespace, final Find find, final Encoder<Document> encoder) {
        this.namespace = namespace;
        this.find = find;
        this.encoder = encoder;
    }


    public Long execute(final ReadBinding binding) {
        return executeWrappedCommandProtocol(namespace, asCommandDocument(), encoder, new DocumentCodec(), binding, transformer());
    }

    @Override
    public MongoFuture<Long> executeAsync(final AsyncReadBinding binding) {
        return executeWrappedCommandProtocolAsync(namespace, asCommandDocument(), encoder, new DocumentCodec(), binding, transformer());
    }

    private Function<CommandResult, Long> transformer() {
        return new Function<CommandResult, Long>() {
            @Override
            public Long apply(final CommandResult result) {
                return ((Number) result.getResponse().get("n")).longValue();
            }
        };
    }

    private Document asCommandDocument() {
        Document document = new Document("count", namespace.getCollectionName());

        if (find.getFilter() != null) {
            document.put("query", find.getFilter());
        }
        if (find.getLimit() > 0) {
            document.put("limit", find.getLimit());
        }
        if (find.getSkip() > 0) {
            document.put("skip", find.getSkip());
        }
        if (find.getOptions().getMaxTime(MILLISECONDS) > 0) {
            document.put("maxTimeMS", find.getOptions().getMaxTime(MILLISECONDS));
        }
        return document;
    }
}
