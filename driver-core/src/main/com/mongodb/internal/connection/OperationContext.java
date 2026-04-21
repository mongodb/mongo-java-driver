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
package com.mongodb.internal.connection;

import com.mongodb.Function;
import com.mongodb.MongoConnectionPoolClearedException;
import com.mongodb.MongoException;
import com.mongodb.ReadConcern;
import com.mongodb.RequestContext;
import com.mongodb.ServerAddress;
import com.mongodb.ServerApi;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.IgnorableRequestContext;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.observability.micrometer.Span;
import com.mongodb.internal.observability.micrometer.TracingManager;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.lang.Nullable;
import com.mongodb.selector.ServerSelector;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.mongodb.MongoException.SYSTEM_OVERLOADED_ERROR_LABEL;
import static java.util.stream.Collectors.toList;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class OperationContext {
    private static final AtomicLong NEXT_ID = new AtomicLong(0);
    private final long id;
    private final ServerDeprioritization serverDeprioritization;
    private final SessionContext sessionContext;
    private final RequestContext requestContext;
    private final TimeoutContext timeoutContext;
    private final TracingManager tracingManager;
    @Nullable
    private final ServerApi serverApi;
    @Nullable
    private final String operationName;
    @Nullable
    private Span tracingSpan;

    public OperationContext(final RequestContext requestContext, final SessionContext sessionContext, final TimeoutContext timeoutContext,
            @Nullable final ServerApi serverApi) {
        this(requestContext, sessionContext, timeoutContext, TracingManager.NO_OP, serverApi, null);
    }

    public OperationContext(final RequestContext requestContext, final SessionContext sessionContext, final TimeoutContext timeoutContext,
            final TracingManager tracingManager,
            @Nullable final ServerApi serverApi,
            @Nullable final String operationName) {
        this(NEXT_ID.incrementAndGet(), requestContext, sessionContext, timeoutContext, new ServerDeprioritization(),
                tracingManager,
                serverApi,
                operationName,
                null);
    }

    public OperationContext(final RequestContext requestContext, final SessionContext sessionContext, final TimeoutContext timeoutContext,
            final TracingManager tracingManager,
            @Nullable final ServerApi serverApi,
            @Nullable final String operationName,
            final ServerDeprioritization serverDeprioritization) {
        this(NEXT_ID.incrementAndGet(), requestContext, sessionContext, timeoutContext, serverDeprioritization,
                tracingManager,
                serverApi,
                operationName,
                null);
    }

    static OperationContext simpleOperationContext(
            final TimeoutSettings timeoutSettings, @Nullable final ServerApi serverApi) {
        return new OperationContext(
                IgnorableRequestContext.INSTANCE,
                NoOpSessionContext.INSTANCE,
                new TimeoutContext(timeoutSettings),
                TracingManager.NO_OP,
                serverApi,
                null
                );
    }

    public static OperationContext simpleOperationContext(final TimeoutContext timeoutContext) {
        return new OperationContext(
                IgnorableRequestContext.INSTANCE,
                NoOpSessionContext.INSTANCE,
                timeoutContext,
                TracingManager.NO_OP,
                null,
                null);
    }

    public OperationContext withSessionContext(final SessionContext sessionContext) {
        return new OperationContext(id, requestContext, sessionContext, timeoutContext, serverDeprioritization, tracingManager, serverApi,
                operationName, tracingSpan);
    }

    public OperationContext withTimeoutContext(final TimeoutContext timeoutContext) {
        return new OperationContext(id, requestContext, sessionContext, timeoutContext, serverDeprioritization, tracingManager, serverApi,
                operationName, tracingSpan);
    }

    public OperationContext withOperationName(final String operationName) {
        return new OperationContext(id, requestContext, sessionContext, timeoutContext, serverDeprioritization, tracingManager, serverApi,
                operationName, tracingSpan);
    }

    /**
     * TODO-JAVA-6058: This method enables overriding the ServerDeprioritization state.
     * It is a temporary solution to handle cases where deprioritization state persists across operations.
     */
    public OperationContext withNewServerDeprioritization() {
        return new OperationContext(id, requestContext, sessionContext, timeoutContext,
                new ServerDeprioritization(serverDeprioritization.enableOverloadRetargeting), tracingManager, serverApi,
                operationName, tracingSpan);
    }

    public long getId() {
        return id;
    }

    public TracingManager getTracingManager() {
        return tracingManager;
    }

    public SessionContext getSessionContext() {
        return sessionContext;
    }

    public RequestContext getRequestContext() {
        return requestContext;
    }

    public TimeoutContext getTimeoutContext() {
        return timeoutContext;
    }

    @Nullable
    public ServerApi getServerApi() {
        return serverApi;
    }

    @Nullable
    public String getOperationName() {
        return operationName;
    }

    @Nullable
    public Span getTracingSpan() {
        return tracingSpan;
    }

    public void setTracingSpan(final Span tracingSpan) {
        this.tracingSpan = tracingSpan;
    }

    private OperationContext(final long id,
            final RequestContext requestContext,
            final SessionContext sessionContext,
            final TimeoutContext timeoutContext,
            final ServerDeprioritization serverDeprioritization,
            final TracingManager tracingManager,
            @Nullable final ServerApi serverApi,
            @Nullable final String operationName,
            @Nullable final Span tracingSpan) {

        this.id = id;
        this.serverDeprioritization = serverDeprioritization;
        this.requestContext = requestContext;
        this.sessionContext = sessionContext;
        this.timeoutContext = timeoutContext;
        this.tracingManager = tracingManager;
        this.serverApi = serverApi;
        this.operationName = operationName;
        this.tracingSpan = tracingSpan;
    }

    /**
     * @return The same {@link ServerDeprioritization} if called on the same {@link OperationContext}.
     */
    public ServerDeprioritization getServerDeprioritization() {
        return serverDeprioritization;
    }

    public OperationContext withNewlyStartedTimeout() {
        return withTimeoutContext(timeoutContext.withNewlyStartedTimeout());
    }

    /**
     * Create a new OperationContext with a SessionContext that does not send a session ID.
     * <p>
     * The driver MUST NOT append a session ID to any command sent during the process of
     * opening and authenticating a connection.
     */
    public OperationContext withConnectionEstablishmentSessionContext() {
        ReadConcern readConcern = getSessionContext().getReadConcern();
        return withSessionContext(new ReadConcernAwareNoOpSessionContext(readConcern));
    }

    public OperationContext withMinRoundTripTime(final ServerDescription serverDescription) {
        return withTimeoutContext(
                timeoutContext.withMinRoundTripTime(TimeUnit.NANOSECONDS.toMillis(serverDescription.getMinRoundTripTimeNanos())));
    }

    public OperationContext withOverride(final TimeoutContextOverride timeoutContextOverrideFunction) {
        return withTimeoutContext(timeoutContextOverrideFunction.apply(timeoutContext));
    }

    public static final class ServerDeprioritization {
        @Nullable
        private ServerAddress candidate;
        @Nullable
        private ClusterType clusterType;
        private final Set<ServerAddress> deprioritized;
        private final boolean enableOverloadRetargeting;

        public ServerDeprioritization() {
            this(false);
        }

        public ServerDeprioritization(final boolean enableOverloadRetargeting) {
            this.enableOverloadRetargeting = enableOverloadRetargeting;
            this.candidate = null;
            this.deprioritized = new HashSet<>();
            this.clusterType = null;
        }

        /**
         * The returned {@link ServerSelector} wraps the provided selector and attempts
         * {@linkplain ServerSelector#select(ClusterDescription) server selection} in two passes:
         * <ol>
         *   <li>First pass: selects using the wrapped selector with only non-deprioritized {@link ServerDescription}s.</li>
         *   <li>Second pass: if the first pass selects no {@link ServerDescription}s,
         *   selects using the wrapped selector again with all {@link ServerDescription}s, including deprioritized ones.</li>
         * </ol>
         */
        ServerSelector apply(final ServerSelector wrappedSelector) {
            return new DeprioritizingSelector(wrappedSelector);
        }

        void updateCandidate(final ServerAddress serverAddress, final ClusterType clusterType) {
            this.candidate = serverAddress;
            this.clusterType = clusterType;
        }

        public void onAttemptFailure(final Throwable failure) {
            if (candidate == null || failure instanceof MongoConnectionPoolClearedException) {
                candidate = null;
                return;
            }

            // As per spec: sharded clusters deprioritize on any error,
            // other topologies deprioritize on overload only when retargeting is enabled.
            boolean isSystemOverloadedError = failure instanceof MongoException
                    && ((MongoException) failure).hasErrorLabel(SYSTEM_OVERLOADED_ERROR_LABEL);

            if (clusterType == ClusterType.SHARDED || (isSystemOverloadedError && enableOverloadRetargeting)) {
                deprioritized.add(candidate);
            }
        }

        /**
         * {@link ServerSelector} requires thread safety, but that is only because a user may specify
         * {@link com.mongodb.connection.ClusterSettings.Builder#serverSelector(ServerSelector)},
         * which indeed may be used concurrently. {@link DeprioritizingSelector} does not need to be thread-safe.
         */
        private final class DeprioritizingSelector implements ServerSelector {
            private final ServerSelector wrappedSelector;

            private DeprioritizingSelector(final ServerSelector wrappedSelector) {
                this.wrappedSelector = wrappedSelector;
            }

            @Override
            public List<ServerDescription> select(final ClusterDescription clusterDescription) {
                List<ServerDescription> serverDescriptions = clusterDescription.getServerDescriptions();

                // TODO-JAVA-5908: Evaluate whether using the early-return optimization has a meaningful performance impact on server selection.
                if (serverDescriptions.size() == 1 || deprioritized.isEmpty()) {
                    return wrappedSelector.select(clusterDescription);
                }

                // TODO-JAVA-5908: Evaluate whether using a loop instead of Stream has a meaningful performance impact on server selection.
                List<ServerDescription> nonDeprioritizedServerDescriptions = serverDescriptions
                        .stream()
                        .filter(serverDescription -> !deprioritized.contains(serverDescription.getAddress()))
                        .collect(toList());

                // TODO-JAVA-5908: Evaluate whether using the early-return optimization has a meaningful performance impact on server selection.
                if (nonDeprioritizedServerDescriptions.isEmpty()) {
                    return wrappedSelector.select(clusterDescription);
                }

                List<ServerDescription> selected = wrappedSelector.select(
                        new ClusterDescription(
                                clusterDescription.getConnectionMode(),
                                clusterDescription.getType(),
                                clusterDescription.getSrvResolutionException(),
                                nonDeprioritizedServerDescriptions,
                                clusterDescription.getClusterSettings(),
                                clusterDescription.getServerSettings()));
                return selected.isEmpty() ? wrappedSelector.select(clusterDescription) : selected;
            }
        }
    }

    public interface TimeoutContextOverride extends Function<TimeoutContext, TimeoutContext> {
    }
}

