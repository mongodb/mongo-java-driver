/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb.internal.operation;

import com.mongodb.MongoNamespace;
import com.mongodb.client.model.Collation;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Decoder;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.operation.AsyncOperationHelper.CommandReadTransformerAsync;
import static com.mongodb.internal.operation.AsyncOperationHelper.executeRetryableReadAsync;
import static com.mongodb.internal.operation.CommandOperationHelper.CommandCreator;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotNull;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotZero;
import static com.mongodb.internal.operation.OperationReadConcernHelper.appendReadConcernToCommand;
import static com.mongodb.internal.operation.SyncOperationHelper.CommandReadTransformer;
import static com.mongodb.internal.operation.SyncOperationHelper.executeRetryableRead;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class CountOperation implements  ReadOperationSimple<Long> {
    private static final String COMMAND_NAME = "count";
    private static final Decoder<BsonDocument> DECODER = new BsonDocumentCodec();
    private final MongoNamespace namespace;
    private boolean retryReads;
    private BsonDocument filter;
    private BsonValue hint;
    private long skip;
    private long limit;
    private Collation collation;

    public CountOperation(final MongoNamespace namespace) {
        this.namespace = notNull("namespace", namespace);
    }

    public BsonDocument getFilter() {
        return filter;
    }

    public CountOperation filter(final BsonDocument filter) {
        this.filter = filter;
        return this;
    }

    public CountOperation retryReads(final boolean retryReads) {
        this.retryReads = retryReads;
        return this;
    }

    public boolean getRetryReads() {
        return retryReads;
    }

    public BsonValue getHint() {
        return hint;
    }

    public CountOperation hint(final BsonValue hint) {
        this.hint = hint;
        return this;
    }

    public long getLimit() {
        return limit;
    }

    public CountOperation limit(final long limit) {
        this.limit = limit;
        return this;
    }

    public long getSkip() {
        return skip;
    }

    public CountOperation skip(final long skip) {
        this.skip = skip;
        return this;
    }

    public Collation getCollation() {
        return collation;
    }

    public CountOperation collation(@Nullable final Collation collation) {
        this.collation = collation;
        return this;
    }

    @Override
    public String getCommandName() {
        return COMMAND_NAME;
    }

    @Override
    public Long execute(final ReadBinding binding) {
        return executeRetryableRead(binding, namespace.getDatabaseName(),
                                    getCommandCreator(), DECODER, transformer(), retryReads);
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<Long> callback) {
        executeRetryableReadAsync(binding, namespace.getDatabaseName(),
                                  getCommandCreator(), DECODER, asyncTransformer(), retryReads, callback);
    }

    private CommandReadTransformer<BsonDocument, Long> transformer() {
        return (result, source, connection) -> (result.getNumber("n")).longValue();
    }

    private CommandReadTransformerAsync<BsonDocument, Long> asyncTransformer() {
        return (result, source, connection) -> (result.getNumber("n")).longValue();
    }

    private CommandCreator getCommandCreator() {
        return (operationContext, serverDescription, connectionDescription) -> {
            BsonDocument document = new BsonDocument(getCommandName(), new BsonString(namespace.getCollectionName()));

            appendReadConcernToCommand(operationContext.getSessionContext(), connectionDescription.getMaxWireVersion(), document);

            putIfNotNull(document, "query", filter);
            putIfNotZero(document, "limit", limit);
            putIfNotZero(document, "skip", skip);
            putIfNotNull(document, "hint", hint);

            if (collation != null) {
                document.put("collation", collation.asDocument());
            }
            return document;
        };
    }
}
