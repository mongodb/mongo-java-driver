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
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.operation.CommandOperationHelper.CommandReadTransformer;
import com.mongodb.internal.operation.CommandOperationHelper.CommandReadTransformerAsync;
import com.mongodb.internal.session.SessionContext;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Decoder;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.operation.CommandOperationHelper.CommandCreator;
import static com.mongodb.internal.operation.CommandOperationHelper.executeRetryableRead;
import static com.mongodb.internal.operation.CommandOperationHelper.executeRetryableReadAsync;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotNull;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotZero;
import static com.mongodb.internal.operation.OperationReadConcernHelper.appendReadConcernToCommand;

public class CountOperation implements AsyncReadOperation<Long>, ReadOperation<Long> {
    private static final Decoder<BsonDocument> DECODER = new BsonDocumentCodec();
    private final MongoNamespace namespace;
    private boolean retryReads;
    private BsonDocument filter;
    private BsonValue hint;
    private long skip;
    private long limit;
    private long maxTimeMS;
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

    public long getMaxTime(final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        return timeUnit.convert(maxTimeMS, TimeUnit.MILLISECONDS);
    }

    public CountOperation maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    public Collation getCollation() {
        return collation;
    }

    public CountOperation collation(final Collation collation) {
        this.collation = collation;
        return this;
    }

    @Override
    public Long execute(final ReadBinding binding) {
        return executeRetryableRead(binding, namespace.getDatabaseName(),
                getCommandCreator(binding.getSessionContext()), DECODER, transformer(), retryReads);
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<Long> callback) {
        executeRetryableReadAsync(binding, namespace.getDatabaseName(), getCommandCreator(binding.getSessionContext()), DECODER,
                asyncTransformer(), retryReads, callback);
    }

    private CommandReadTransformer<BsonDocument, Long> transformer() {
        return (result, source, connection) -> (result.getNumber("n")).longValue();
    }

    private CommandReadTransformerAsync<BsonDocument, Long> asyncTransformer() {
        return (result, source, connection) -> (result.getNumber("n")).longValue();
    }

    private CommandCreator getCommandCreator(final SessionContext sessionContext) {
        return (serverDescription, connectionDescription) -> getCommand(sessionContext, connectionDescription);
    }

    private BsonDocument getCommand(final SessionContext sessionContext, final ConnectionDescription connectionDescription) {
        BsonDocument document = new BsonDocument("count", new BsonString(namespace.getCollectionName()));

        appendReadConcernToCommand(sessionContext, connectionDescription.getMaxWireVersion(), document);

        putIfNotNull(document, "query", filter);
        putIfNotZero(document, "limit", limit);
        putIfNotZero(document, "skip", skip);
        putIfNotNull(document, "hint", hint);
        putIfNotZero(document, "maxTimeMS", maxTimeMS);

        if (collation != null) {
            document.put("collation", collation.asDocument());
        }
        return document;
    }
}
