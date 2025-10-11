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
import com.mongodb.client.cursor.TimeoutMode;
import com.mongodb.client.model.Collation;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.client.model.AggregationLevel;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.codecs.Decoder;

import java.util.List;

import static com.mongodb.internal.connection.CommandHelper.applyMaxTimeMS;
import static com.mongodb.internal.operation.ExplainHelper.asExplainCommand;
import static com.mongodb.internal.operation.ServerVersionHelper.UNKNOWN_WIRE_VERSION;

/**
 * An operation that executes an aggregation query.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class AggregateOperation<T> implements ReadOperationExplainable<T> {
    private final AggregateOperationImpl<T> wrapped;

    public AggregateOperation(final MongoNamespace namespace, final List<BsonDocument> pipeline, final Decoder<T> decoder) {
        this(namespace, pipeline, decoder, AggregationLevel.COLLECTION);
    }

    public AggregateOperation(final MongoNamespace namespace, final List<BsonDocument> pipeline, final Decoder<T> decoder,
            final AggregationLevel aggregationLevel) {
        this.wrapped = new AggregateOperationImpl<>(namespace, pipeline, decoder, aggregationLevel);
    }

    public List<BsonDocument> getPipeline() {
        return wrapped.getPipeline();
    }

    public Boolean getAllowDiskUse() {
        return wrapped.getAllowDiskUse();
    }

    public AggregateOperation<T> allowDiskUse(@Nullable final Boolean allowDiskUse) {
        wrapped.allowDiskUse(allowDiskUse);
        return this;
    }

    public Integer getBatchSize() {
        return wrapped.getBatchSize();
    }

    public AggregateOperation<T> batchSize(@Nullable final Integer batchSize) {
        wrapped.batchSize(batchSize);
        return this;
    }

    public Collation getCollation() {
        return wrapped.getCollation();
    }

    public AggregateOperation<T> collation(@Nullable final Collation collation) {
        wrapped.collation(collation);
        return this;
    }

   @Nullable
    public BsonValue getComment() {
        return wrapped.getComment();
    }

    public AggregateOperation<T> comment(@Nullable final BsonValue comment) {
        wrapped.comment(comment);
        return this;
    }

    public AggregateOperation<T> let(@Nullable final BsonDocument variables) {
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

    @Nullable
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

    @Nullable
    public BsonValue getHintBsonValue() {
        return wrapped.getHint();
    }

    public AggregateOperation<T> hint(@Nullable final BsonValue hint) {
        wrapped.hint(hint);
        return this;
    }

    public AggregateOperation<T> timeoutMode(@Nullable final TimeoutMode timeoutMode) {
        wrapped.timeoutMode(timeoutMode);
        return this;
    }

    @Override
    public String getCommandName() {
        return wrapped.getCommandName();
    }

    @Override
    public BatchCursor<T> execute(final ReadBinding binding) {
        return wrapped.execute(binding);
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<AsyncBatchCursor<T>> callback) {
        wrapped.executeAsync(binding, callback);
    }

    @Override
    public <R> ReadOperationSimple<R> asExplainableOperation(@Nullable final ExplainVerbosity verbosity, final Decoder<R> resultDecoder) {
        return createExplainableOperation(verbosity, resultDecoder);
    }

    <R> CommandReadOperation<R> createExplainableOperation(@Nullable final ExplainVerbosity verbosity, final Decoder<R> resultDecoder) {
        return new CommandReadOperation<>(getNamespace().getDatabaseName(), wrapped.getCommandName(),
                (operationContext, serverDescription, connectionDescription) -> {
                    BsonDocument command = wrapped.getCommand(operationContext, UNKNOWN_WIRE_VERSION);
                    applyMaxTimeMS(operationContext.getTimeoutContext(), command);
                    return asExplainCommand(command, verbosity);
                }, resultDecoder);
    }

    @Override
    public MongoNamespace getNamespace() {
        return wrapped.getNamespace();
    }
}
