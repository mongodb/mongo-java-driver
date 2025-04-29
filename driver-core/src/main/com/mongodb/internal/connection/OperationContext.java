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

import com.mongodb.MongoConnectionPoolClearedException;
import com.mongodb.RequestContext;
import com.mongodb.ServerAddress;
import com.mongodb.ServerApi;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.IgnorableRequestContext;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.VisibleForTesting;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.internal.tracing.TracingManager;
import com.mongodb.lang.Nullable;
import com.mongodb.selector.ServerSelector;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

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
                operationName);
    }

    public static OperationContext simpleOperationContext(
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
                operationName);
    }

    public OperationContext withTimeoutContext(final TimeoutContext timeoutContext) {
        return new OperationContext(id, requestContext, sessionContext, timeoutContext, serverDeprioritization, tracingManager, serverApi,
                operationName);
    }

    public OperationContext withOperationName(final String operationName) {
        return new OperationContext(id, requestContext, sessionContext, timeoutContext, serverDeprioritization, tracingManager, serverApi,
                operationName);
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

    @VisibleForTesting(otherwise = VisibleForTesting.AccessModifier.PRIVATE)
    public OperationContext(final long id,
            final RequestContext requestContext,
            final SessionContext sessionContext,
            final TimeoutContext timeoutContext,
            final ServerDeprioritization serverDeprioritization,
            final TracingManager tracingManager,
            @Nullable final ServerApi serverApi,
            @Nullable final String operationName) {
        this.id = id;
        this.serverDeprioritization = serverDeprioritization;
        this.requestContext = requestContext;
        this.sessionContext = sessionContext;
        this.timeoutContext = timeoutContext;
        this.tracingManager = tracingManager;
        this.serverApi = serverApi;
        this.operationName = operationName;
    }

    @VisibleForTesting(otherwise = VisibleForTesting.AccessModifier.PRIVATE)
    public OperationContext(final long id,
            final RequestContext requestContext,
            final SessionContext sessionContext,
            final TimeoutContext timeoutContext,
            final TracingManager tracingManager,
            @Nullable final ServerApi serverApi,
            @Nullable final String operationName) {
        this.id = id;
        this.serverDeprioritization = new ServerDeprioritization();
        this.requestContext = requestContext;
        this.sessionContext = sessionContext;
        this.timeoutContext = timeoutContext;
        this.tracingManager = tracingManager;
        this.serverApi = serverApi;
        this.operationName = operationName;
    }


    /**
     * @return The same {@link ServerDeprioritization} if called on the same {@link OperationContext}.
     */
    public ServerDeprioritization getServerDeprioritization() {
        return serverDeprioritization;
    }

    public static final class ServerDeprioritization {
        @Nullable
        private ServerAddress candidate;
        private final Set<ServerAddress> deprioritized;
        private final DeprioritizingSelector selector;

        private ServerDeprioritization() {
            candidate = null;
            deprioritized = new HashSet<>();
            selector = new DeprioritizingSelector();
        }

        /**
         * The returned {@link ServerSelector} tries to {@linkplain ServerSelector#select(ClusterDescription) select}
         * only the {@link ServerDescription}s that do not have deprioritized {@link ServerAddress}es.
         * If no such {@link ServerDescription} can be selected, then it selects {@link ClusterDescription#getServerDescriptions()}.
         */
        ServerSelector getServerSelector() {
            return selector;
        }

        void updateCandidate(final ServerAddress serverAddress) {
            candidate = serverAddress;
        }

        public void onAttemptFailure(final Throwable failure) {
            if (candidate == null || failure instanceof MongoConnectionPoolClearedException) {
                candidate = null;
                return;
            }
            deprioritized.add(candidate);
        }

        /**
         * {@link ServerSelector} requires thread safety, but that is only because a user may specify
         * {@link com.mongodb.connection.ClusterSettings.Builder#serverSelector(ServerSelector)},
         * which indeed may be used concurrently. {@link DeprioritizingSelector} does not need to be thread-safe.
         */
        private final class DeprioritizingSelector implements ServerSelector {
            private DeprioritizingSelector() {
            }

            @Override
            public List<ServerDescription> select(final ClusterDescription clusterDescription) {
                List<ServerDescription> serverDescriptions = clusterDescription.getServerDescriptions();
                if (!isEnabled(clusterDescription.getType())) {
                    return serverDescriptions;
                }
                List<ServerDescription> nonDeprioritizedServerDescriptions = serverDescriptions
                        .stream()
                        .filter(serverDescription -> !deprioritized.contains(serverDescription.getAddress()))
                        .collect(toList());
                return nonDeprioritizedServerDescriptions.isEmpty() ? serverDescriptions : nonDeprioritizedServerDescriptions;
            }

            private boolean isEnabled(final ClusterType clusterType) {
                return clusterType == ClusterType.SHARDED;
            }
        }
    }
}

