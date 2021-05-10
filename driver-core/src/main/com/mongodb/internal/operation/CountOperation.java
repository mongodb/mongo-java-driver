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
import com.mongodb.internal.ClientSideOperationTimeout;
import com.mongodb.internal.ClientSideOperationTimeoutFactory;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.operation.AsyncCommandOperationHelper.CommandReadTransformerAsync;
import com.mongodb.internal.operation.SyncCommandOperationHelper.CommandReadTransformer;
import com.mongodb.internal.session.SessionContext;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Decoder;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.operation.AsyncCommandOperationHelper.executeCommandAsync;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotNull;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotZero;
import static com.mongodb.internal.operation.OperationHelper.validateReadConcernAndCollation;
import static com.mongodb.internal.operation.OperationReadConcernHelper.appendReadConcernToCommand;
import static com.mongodb.internal.operation.SyncCommandOperationHelper.executeCommand;

public class CountOperation implements AsyncReadOperation<Long>, ReadOperation<Long> {
    private static final Decoder<BsonDocument> DECODER = new BsonDocumentCodec();
    private final ClientSideOperationTimeoutFactory clientSideOperationTimeoutFactory;
    private final MongoNamespace namespace;
    private boolean retryReads;
    private BsonDocument filter;
    private BsonValue hint;
    private long skip;
    private long limit;
    private Collation collation;

    /**
     * Construct an instance.
     *
     * @param clientSideOperationTimeoutFactory the client side operation timeout factory
     * @param namespace the database and collection namespace for the operation.
     */
    public CountOperation(final ClientSideOperationTimeoutFactory clientSideOperationTimeoutFactory, final MongoNamespace namespace) {
        this.clientSideOperationTimeoutFactory = notNull("clientSideOperationTimeoutFactory", clientSideOperationTimeoutFactory);
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

    public CountOperation collation(final Collation collation) {
        this.collation = collation;
        return this;
    }

    @Override
    public Long execute(final ReadBinding binding) {
        return executeCommand(clientSideOperationTimeoutFactory.create(), binding, namespace.getDatabaseName(),
                getCommandCreator(binding.getSessionContext()), DECODER, transformer(), retryReads);
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<Long> callback) {
        executeCommandAsync(clientSideOperationTimeoutFactory.create(), binding, namespace.getDatabaseName(),
                getCommandCreator(binding.getSessionContext()), DECODER, asyncTransformer(), retryReads, callback);
    }

    private CommandReadTransformer<BsonDocument, Long> transformer() {
        return (clientSideOperationTimeout, source, connection, result) -> (result.getNumber("n")).longValue();
    }

    private CommandReadTransformerAsync<BsonDocument, Long> asyncTransformer() {
        return (clientSideOperationTimeout, source, connection, result) -> (result.getNumber("n")).longValue();
    }

    private CommandCreator getCommandCreator(final SessionContext sessionContext) {
        return (clientSideOperationTimeout, serverDescription, connectionDescription) -> {
            validateReadConcernAndCollation(connectionDescription, sessionContext.getReadConcern(), collation);
            return getCommand(clientSideOperationTimeout, sessionContext);
        };
    }

    private BsonDocument getCommand(final ClientSideOperationTimeout clientSideOperationTimeout, final SessionContext sessionContext) {
        BsonDocument document = new BsonDocument("count", new BsonString(namespace.getCollectionName()));

        appendReadConcernToCommand(sessionContext, document);

        putIfNotNull(document, "query", filter);
        putIfNotZero(document, "limit", limit);
        putIfNotZero(document, "skip", skip);
        putIfNotNull(document, "hint", hint);
        putIfNotZero(document, "maxTimeMS", clientSideOperationTimeout.getMaxTimeMS());
        if (collation != null) {
            document.put("collation", collation.asDocument());
        }
        return document;
    }
}
