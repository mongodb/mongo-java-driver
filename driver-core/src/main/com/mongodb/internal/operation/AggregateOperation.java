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

import com.mongodb.ExplainVerbosity;
import com.mongodb.MongoNamespace;
import com.mongodb.client.model.Collation;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.client.model.AggregationLevel;
import com.mongodb.internal.connection.NoOpSessionContext;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.codecs.Decoder;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.internal.operation.ExplainHelper.asExplainCommand;
import static com.mongodb.internal.operation.ServerVersionHelper.MIN_WIRE_VERSION;

/**
 * An operation that executes an aggregation query.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class AggregateOperation<T> implements AsyncExplainableReadOperation<AsyncBatchCursor<T>>, ExplainableReadOperation<BatchCursor<T>> {
    private final AggregateOperationImpl<T> wrapped;

    public AggregateOperation(final MongoNamespace namespace, final List<BsonDocument> pipeline, final Decoder<T> decoder) {
        this(namespace, pipeline, decoder, AggregationLevel.COLLECTION);
    }

    public AggregateOperation(final MongoNamespace namespace, final List<BsonDocument> pipeline, final Decoder<T> decoder,
                              final AggregationLevel aggregationLevel) {
        this.wrapped = new AggregateOperationImpl<T>(namespace, pipeline, decoder, aggregationLevel);
    }

    public List<BsonDocument> getPipeline() {
        return wrapped.getPipeline();
    }

    public Boolean getAllowDiskUse() {
        return wrapped.getAllowDiskUse();
    }

    public AggregateOperation<T> allowDiskUse(final Boolean allowDiskUse) {
        wrapped.allowDiskUse(allowDiskUse);
        return this;
    }

    public Integer getBatchSize() {
        return wrapped.getBatchSize();
    }

    public AggregateOperation<T> batchSize(final Integer batchSize) {
        wrapped.batchSize(batchSize);
        return this;
    }

    public long getMaxAwaitTime(final TimeUnit timeUnit) {
        return wrapped.getMaxAwaitTime(timeUnit);
    }

    public AggregateOperation<T> maxAwaitTime(final long maxAwaitTime, final TimeUnit timeUnit) {
        wrapped.maxAwaitTime(maxAwaitTime, timeUnit);
        return this;
    }

    public long getMaxTime(final TimeUnit timeUnit) {
        return wrapped.getMaxTime(timeUnit);
    }

    public AggregateOperation<T> maxTime(final long maxTime, final TimeUnit timeUnit) {
        wrapped.maxTime(maxTime, timeUnit);
        return this;
    }

    public Collation getCollation() {
        return wrapped.getCollation();
    }

    public AggregateOperation<T> collation(final Collation collation) {
        wrapped.collation(collation);
        return this;
    }

    public BsonValue getComment() {
        return wrapped.getComment();
    }

    public AggregateOperation<T> comment(final BsonValue comment) {
        wrapped.comment(comment);
        return this;
    }

    public AggregateOperation<T> let(final BsonDocument variables) {
        wrapped.let(variables);
        return this;
    }

    public AggregateOperation<T> retryReads(final boolean retryReads) {
        wrapped.retryReads(retryReads);
        return this;
    }

    public boolean getRetryReads() {
        return wrapped.getRetryReads();
    }

    public BsonDocument getHint() {
        BsonValue hint = wrapped.getHint();
        if (hint == null) {
            return null;
        }
        if (!hint.isDocument()) {
            throw new IllegalArgumentException("Hint is not a BsonDocument please use the #getHintBsonValue() method. ");
        }
        return hint.asDocument();
    }

    public BsonValue getHintBsonValue() {
        return wrapped.getHint();
    }

    public AggregateOperation<T> hint(final BsonValue hint) {
        wrapped.hint(hint);
        return this;
    }

    @Override
    public BatchCursor<T> execute(final ReadBinding binding) {
        return wrapped.execute(binding);
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<AsyncBatchCursor<T>> callback) {
        wrapped.executeAsync(binding, callback);
    }

    public <R> ReadOperation<R> asExplainableOperation(@Nullable final ExplainVerbosity verbosity, final Decoder<R> resultDecoder) {
        return new CommandReadOperation<R>(getNamespace().getDatabaseName(),
                asExplainCommand(wrapped.getCommand(NoOpSessionContext.INSTANCE, MIN_WIRE_VERSION), verbosity),
                resultDecoder);
    }

    public <R> AsyncReadOperation<R> asAsyncExplainableOperation(@Nullable final ExplainVerbosity verbosity,
                                                                 final Decoder<R> resultDecoder) {
        return new CommandReadOperation<R>(getNamespace().getDatabaseName(),
                asExplainCommand(wrapped.getCommand(NoOpSessionContext.INSTANCE, MIN_WIRE_VERSION), verbosity),
                resultDecoder);
    }


    MongoNamespace getNamespace() {
        return wrapped.getNamespace();
    }

    Decoder<T> getDecoder() {
        return wrapped.getDecoder();
    }
}
