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

import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.codecs.Decoder;

import static com.mongodb.internal.operation.AsyncOperationHelper.executeRetryableReadAsync;
import static com.mongodb.internal.operation.SyncOperationHelper.executeRetryableRead;

/**
 * An operation that executes an arbitrary command that reads from the server.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class CommandReadOperation<T> extends AbstractCommandReadOperation<T> {
    private final boolean retryReads;
    private final boolean retryWrites;
    @Nullable
    private final Integer maxAdaptiveRetriesSetting;

    public CommandReadOperation(final String databaseName, final BsonDocument command, final Decoder<T> decoder,
                                final boolean retryReads, final boolean retryWrites,
                                @Nullable final Integer maxAdaptiveRetriesSetting) {
        super(databaseName, command, decoder);
        this.retryReads = retryReads;
        this.retryWrites = retryWrites;
        this.maxAdaptiveRetriesSetting = maxAdaptiveRetriesSetting;
    }

    /**
     * Convenience constructor for callers (mostly tests) that do not require overload retry.
     */
    public CommandReadOperation(final String databaseName, final BsonDocument command, final Decoder<T> decoder) {
        this(databaseName, command, decoder, false, false, null);
    }

    @Override
    public T execute(final ReadBinding binding, final OperationContext operationContext) {
        return executeRetryableRead(binding,
                operationContext,
                getDatabaseName(),
                getCommandCreator(),
                getDecoder(),
                AbstractCommandReadOperation.transformer(),
                createRetryPolicy());
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final OperationContext operationContext,
                             final SingleResultCallback<T> callback) {
        executeRetryableReadAsync(binding,
                operationContext,
                getDatabaseName(),
                getCommandCreator(),
                getDecoder(),
                AbstractCommandReadOperation.asyncTransformer(),
                createRetryPolicy(),
                callback);
    }

    private SpecRetryPolicy.IndividualPolicies createRetryPolicy() {
        boolean retryPolicyEnabled = retryReads && retryWrites;
        return new SpecRetryPolicy.IndividualPolicies(retryPolicyEnabled)
                .includeOverload(maxAdaptiveRetriesSetting, SpecRetryPolicy.ErrorPropagation.AS_WRITE_POLICY);
    }
}
