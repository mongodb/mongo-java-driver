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
package com.mongodb.reactivestreams.client.internal;

import com.mongodb.MongoClientException;
import com.mongodb.MongoException;
import com.mongodb.MongoInternalException;
import com.mongodb.MongoQueryException;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.RequestContext;
import com.mongodb.internal.IgnorableRequestContext;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.binding.AsyncClusterAwareReadWriteBinding;
import com.mongodb.internal.binding.AsyncClusterBinding;
import com.mongodb.internal.binding.AsyncReadWriteBinding;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.internal.connection.ReadConcernAwareNoOpSessionContext;
import com.mongodb.internal.operation.OperationHelper;
import com.mongodb.internal.operation.ReadOperation;
import com.mongodb.internal.operation.WriteOperation;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.ReactiveContextProvider;
import com.mongodb.reactivestreams.client.internal.crypt.Crypt;
import com.mongodb.reactivestreams.client.internal.crypt.CryptBinding;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Mono;

import java.util.Objects;

import static com.mongodb.MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL;
import static com.mongodb.MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL;
import static com.mongodb.ReadPreference.primary;
import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.TimeoutContext.createTimeoutContext;
import static com.mongodb.reactivestreams.client.internal.MongoOperationPublisher.sinkToCallback;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class OperationExecutorImpl implements OperationExecutor {

    private final MongoClientImpl mongoClient;
    private final ClientSessionHelper clientSessionHelper;
    @Nullable
    private final ReactiveContextProvider contextProvider;
    private final TimeoutSettings timeoutSettings;

    OperationExecutorImpl(final MongoClientImpl mongoClient, final ClientSessionHelper clientSessionHelper,
            final TimeoutSettings timeoutSettings, @Nullable final ReactiveContextProvider contextProvider) {
        this.mongoClient = mongoClient;
        this.clientSessionHelper = clientSessionHelper;
        this.timeoutSettings = timeoutSettings;
        this.contextProvider = contextProvider;
    }

    @Override
    public <T> Mono<T> execute(final ReadOperation<?, T> operation, final ReadPreference readPreference, final ReadConcern readConcern,
            @Nullable final ClientSession session) {
        isTrue("open", !mongoClient.getCluster().isClosed());
        notNull("operation", operation);
        notNull("readPreference", readPreference);
        notNull("readConcern", readConcern);

        if (session != null) {
            session.notifyOperationInitiated(operation);
        }

        return Mono.from(subscriber ->
                clientSessionHelper.withClientSession(session, this)
                        .map(clientSession -> getReadWriteBinding(getContext(subscriber),
                                readPreference, readConcern, clientSession, session == null, operation.getCommandName()))
                        .flatMap(binding -> {
                            if (session != null && session.hasActiveTransaction() && !binding.getReadPreference().equals(primary())) {
                                binding.release();
                                return Mono.error(new MongoClientException("Read preference in a transaction must be primary"));
                            } else {
                                return Mono.<T>create(sink -> operation.executeAsync(binding, (result, t) -> {
                                    try {
                                        binding.release();
                                    } finally {
                                        sinkToCallback(sink).onResult(result, t);
                                    }
                                })).doOnError((t) -> {
                                    Throwable exceptionToHandle = t instanceof MongoException ? OperationHelper.unwrap((MongoException) t) : t;
                                    labelException(session, exceptionToHandle);
                                    unpinServerAddressOnTransientTransactionError(session, exceptionToHandle);
                                });
                            }
                        }).subscribe(subscriber)
        );
    }

    @Override
    public <T> Mono<T> execute(final WriteOperation<T> operation, final ReadConcern readConcern,
            @Nullable final ClientSession session) {
        isTrue("open", !mongoClient.getCluster().isClosed());
        notNull("operation", operation);
        notNull("readConcern", readConcern);

        if (session != null) {
            session.notifyOperationInitiated(operation);
        }

        return Mono.from(subscriber ->
                clientSessionHelper.withClientSession(session, this)
                        .map(clientSession -> getReadWriteBinding(getContext(subscriber),
                                primary(), readConcern, clientSession, session == null, operation.getCommandName()))
                        .flatMap(binding ->
                                Mono.<T>create(sink -> operation.executeAsync(binding, (result, t) -> {
                                    try {
                                        binding.release();
                                    } finally {
                                        sinkToCallback(sink).onResult(result, t);
                                    }
                                })).doOnError((t) -> {
                                    Throwable exceptionToHandle = t instanceof MongoException ? OperationHelper.unwrap((MongoException) t) : t;
                                    labelException(session, exceptionToHandle);
                                    unpinServerAddressOnTransientTransactionError(session, exceptionToHandle);
                                })
                        ).subscribe(subscriber)
        );
    }

    @Override
    public OperationExecutor withTimeoutSettings(final TimeoutSettings newTimeoutSettings) {
        if (Objects.equals(timeoutSettings, newTimeoutSettings)) {
            return this;
        }
        return new OperationExecutorImpl(mongoClient, clientSessionHelper, newTimeoutSettings, contextProvider);
    }

    @Override
    public TimeoutSettings getTimeoutSettings() {
        return timeoutSettings;
    }

    private <T> RequestContext getContext(final Subscriber<T> subscriber) {
        RequestContext context = null;
        if (contextProvider != null) {
            context = contextProvider.getContext(subscriber);
        }
        return context == null ? IgnorableRequestContext.INSTANCE : context;
    }

    private void labelException(@Nullable final ClientSession session, @Nullable final Throwable t) {
        if (session != null && session.hasActiveTransaction()
                && (t instanceof MongoSocketException || t instanceof MongoTimeoutException
                || (t instanceof MongoQueryException && ((MongoQueryException) t).getErrorCode() == 91))
                && !((MongoException) t).hasErrorLabel(UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL)) {
            ((MongoException) t).addLabel(TRANSIENT_TRANSACTION_ERROR_LABEL);
        }
    }

    private void unpinServerAddressOnTransientTransactionError(@Nullable final ClientSession session,
            @Nullable final Throwable throwable) {
        if (session != null && throwable instanceof MongoException
                && ((MongoException) throwable).hasErrorLabel(TRANSIENT_TRANSACTION_ERROR_LABEL)) {
            session.clearTransactionContext();
        }
    }

    private AsyncReadWriteBinding getReadWriteBinding(final RequestContext requestContext,
            final ReadPreference readPreference, final ReadConcern readConcern, final ClientSession session,
            final boolean ownsSession, final String commandName) {
        notNull("readPreference", readPreference);
        AsyncClusterAwareReadWriteBinding readWriteBinding = new AsyncClusterBinding(mongoClient.getCluster(),
                getReadPreferenceForBinding(readPreference, session), readConcern,
                getOperationContext(requestContext, session, readConcern, commandName));

        Crypt crypt = mongoClient.getCrypt();
        if (crypt != null) {
            readWriteBinding = new CryptBinding(readWriteBinding, crypt);
        }

        AsyncClusterAwareReadWriteBinding asyncReadWriteBinding = readWriteBinding;
        if (session != null) {
            return new ClientSessionBinding(session, ownsSession, asyncReadWriteBinding);
        } else {
            return asyncReadWriteBinding;
        }
    }

    private OperationContext getOperationContext(final RequestContext requestContext, final ClientSession session,
            final ReadConcern readConcern, final String commandName) {
        return new OperationContext(
                requestContext,
                new ReadConcernAwareNoOpSessionContext(readConcern),
                createTimeoutContext(session, timeoutSettings),
                mongoClient.getSettings().getServerApi(),
                commandName);
    }

    private ReadPreference getReadPreferenceForBinding(final ReadPreference readPreference, @Nullable final ClientSession session) {
        if (session == null) {
            return readPreference;
        }
        if (session.hasActiveTransaction()) {
            ReadPreference readPreferenceForBinding = session.getTransactionOptions().getReadPreference();
            if (readPreferenceForBinding == null) {
                throw new MongoInternalException("Invariant violated.  Transaction options read preference can not be null");
            }
            return readPreferenceForBinding;
        }
        return readPreference;
    }
}
