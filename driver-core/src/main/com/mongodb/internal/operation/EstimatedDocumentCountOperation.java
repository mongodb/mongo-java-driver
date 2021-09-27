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

import com.mongodb.MongoCommandException;
import com.mongodb.MongoNamespace;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.connection.QueryResult;
import com.mongodb.internal.operation.CommandOperationHelper.CommandReadTransformer;
import com.mongodb.internal.operation.CommandOperationHelper.CommandReadTransformerAsync;
import com.mongodb.internal.session.SessionContext;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Decoder;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.operation.CommandOperationHelper.CommandCreator;
import static com.mongodb.internal.operation.CommandOperationHelper.executeRetryableRead;
import static com.mongodb.internal.operation.CommandOperationHelper.executeRetryableReadAsync;
import static com.mongodb.internal.operation.CommandOperationHelper.isNamespaceError;
import static com.mongodb.internal.operation.CommandOperationHelper.rethrowIfNotNamespaceError;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotZero;
import static com.mongodb.internal.operation.OperationHelper.cursorDocumentToQueryResult;
import static com.mongodb.internal.operation.OperationHelper.validateReadConcern;
import static com.mongodb.internal.operation.OperationReadConcernHelper.appendReadConcernToCommand;
import static com.mongodb.internal.operation.ServerVersionHelper.serverIsAtLeastVersionFiveDotZero;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class EstimatedDocumentCountOperation implements AsyncReadOperation<Long>, ReadOperation<Long> {
    private static final Decoder<BsonDocument> DECODER = new BsonDocumentCodec();
    private final MongoNamespace namespace;
    private boolean retryReads;
    private long maxTimeMS;

    public EstimatedDocumentCountOperation(final MongoNamespace namespace) {
        this.namespace = notNull("namespace", namespace);
    }

    public EstimatedDocumentCountOperation retryReads(final boolean retryReads) {
        this.retryReads = retryReads;
        return this;
    }

    public EstimatedDocumentCountOperation maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    @Override
    public Long execute(final ReadBinding binding) {
        try {
            return executeRetryableRead(binding, namespace.getDatabaseName(), getCommandCreator(binding.getSessionContext()),
                    CommandResultDocumentCodec.create(DECODER, singletonList("firstBatch")), transformer(), retryReads);
        } catch (MongoCommandException e) {
            return rethrowIfNotNamespaceError(e, 0L);
        }
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<Long> callback) {
        executeRetryableReadAsync(binding, namespace.getDatabaseName(), getCommandCreator(binding.getSessionContext()),
                CommandResultDocumentCodec.create(DECODER, singletonList("firstBatch")), asyncTransformer(), retryReads,
                (result, t) -> {
                    if (isNamespaceError(t)) {
                        callback.onResult(0L, null);
                    } else {
                        callback.onResult(result, t);
                    }
                });
    }

    private CommandReadTransformer<BsonDocument, Long> transformer() {
        return (result, source, connection) -> transformResult(result, connection.getDescription());
    }

    private CommandReadTransformerAsync<BsonDocument, Long> asyncTransformer() {
        return (result, source, connection) -> transformResult(result, connection.getDescription());
    }

    private long transformResult(final BsonDocument result, final ConnectionDescription connectionDescription) {
        if (serverIsAtLeastVersionFiveDotZero(connectionDescription)) {
            QueryResult<BsonDocument> queryResult = cursorDocumentToQueryResult(result.getDocument("cursor"),
                    connectionDescription.getServerAddress());
            return queryResult.getResults().get(0).getNumber("n").longValue();
        } else {
            return (result.getNumber("n")).longValue();
        }
    }

    private CommandCreator getCommandCreator(final SessionContext sessionContext) {
        return (serverDescription, connectionDescription) -> {
            if (serverIsAtLeastVersionFiveDotZero(connectionDescription)) {
                return getAggregateCommand(sessionContext, connectionDescription);
            } else {
                validateReadConcern(connectionDescription, sessionContext.getReadConcern());
                return getCountCommand(sessionContext, connectionDescription);
            }
        };
    }

    private BsonDocument getAggregateCommand(final SessionContext sessionContext, final ConnectionDescription connectionDescription) {
        BsonDocument document = new BsonDocument("aggregate", new BsonString(namespace.getCollectionName()))
                .append("cursor", new BsonDocument())
                .append("pipeline", new BsonArray(asList(
                     new BsonDocument("$collStats", new BsonDocument("count", new BsonDocument())),
                     new BsonDocument("$group", new BsonDocument("_id", new BsonInt32(1))
                             .append("n", new BsonDocument("$sum", new BsonString("$count")))
                ))));

        appendReadConcernToCommand(sessionContext, connectionDescription.getMaxWireVersion(), document);
        putIfNotZero(document, "maxTimeMS", maxTimeMS);
        return document;
    }

    private BsonDocument getCountCommand(final SessionContext sessionContext, final ConnectionDescription connectionDescription) {
        BsonDocument document = new BsonDocument("count", new BsonString(namespace.getCollectionName()));

        appendReadConcernToCommand(sessionContext, connectionDescription.getMaxWireVersion(), document);
        putIfNotZero(document, "maxTimeMS", maxTimeMS);
        return document;
    }
}
