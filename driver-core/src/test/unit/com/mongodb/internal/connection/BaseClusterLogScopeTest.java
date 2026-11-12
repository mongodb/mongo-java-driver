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

import com.mongodb.MongoTimeoutException;
import com.mongodb.MongoNamespace;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.logging.LogMessage;
import com.mongodb.internal.observability.micrometer.Span;
import com.mongodb.internal.observability.micrometer.TraceContext;
import com.mongodb.internal.selector.ReadPreferenceServerSelector;
import com.mongodb.lang.Nullable;
import com.mongodb.logging.TestLoggingInterceptor;
import com.mongodb.observability.micrometer.MongodbObservationContext;
import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mongodb.ClusterFixture.createOperationContext;
import static com.mongodb.ReadPreference.primary;
import static com.mongodb.connection.ClusterConnectionMode.SINGLE;
import static com.mongodb.connection.ClusterType.STANDALONE;
import static com.mongodb.internal.logging.LogMessage.Component.SERVER_SELECTION;
import static com.mongodb.internal.logging.LogMessage.Level.DEBUG;
import static org.junit.jupiter.api.Assertions.assertEquals;

final class BaseClusterLogScopeTest {

    /** Records, per log message id, whether the span scope was open on the thread that emitted it. */
    private static final class ScopeRecordingInterceptor extends TestLoggingInterceptor {
        private final ScopeCountingSpan span;
        private final Map<String, Boolean> scopeOpenAtMessage = new ConcurrentHashMap<>();

        ScopeRecordingInterceptor(final String applicationName, final ScopeCountingSpan span) {
            super(applicationName, new LoggingFilter(Collections.singletonMap(SERVER_SELECTION, DEBUG)));
            this.span = span;
        }

        @Override
        public synchronized void intercept(final LogMessage message) {
            super.intercept(message);
            scopeOpenAtMessage.put(message.getMessageId(), span.isScopeOpen());
        }
    }

    @Test
    void serverSelectionMessagesAreEmittedInsideTheOperationSpanScope() {
        String applicationName = "BaseClusterLogScopeTest";
        ClusterId clusterId = new ClusterId(applicationName);
        ScopeCountingSpan span = new ScopeCountingSpan();
        OperationContext operationContext = createOperationContext();
        operationContext.setTracingSpan(span);
        ServerAddress address = new ServerAddress();
        ClusterDescription description = new ClusterDescription(SINGLE, STANDALONE, Collections.singletonList(
                ServerDescription.builder().address(address).ok(true).state(com.mongodb.connection.ServerConnectionState.CONNECTED).build()));
        ReadPreferenceServerSelector selector = new ReadPreferenceServerSelector(primary());

        try (ScopeRecordingInterceptor interceptor = new ScopeRecordingInterceptor(applicationName, span)) {
            BaseCluster.logServerSelectionStarted(operationContext, clusterId, selector, description);
            BaseCluster.logServerSelectionWaiting(operationContext, clusterId,
                    operationContext.getTimeoutContext().computeServerSelectionTimeout(), selector, description);
            BaseCluster.logServerSelectionFailed(operationContext, clusterId, new MongoTimeoutException("test"), selector, description);
            BaseCluster.logServerSelectionSucceeded(operationContext, clusterId, address, selector, description);

            assertEquals(Boolean.TRUE, interceptor.scopeOpenAtMessage.get("Server selection started"));
            assertEquals(Boolean.TRUE, interceptor.scopeOpenAtMessage.get("Waiting for suitable server to become available"));
            assertEquals(Boolean.TRUE, interceptor.scopeOpenAtMessage.get("Server selection failed"));
            assertEquals(Boolean.TRUE, interceptor.scopeOpenAtMessage.get("Server selection succeeded"));
            assertEquals(span.openCount.get(), span.closeCount.get(), "scopes must be balanced");
        }
    }

    static final class ScopeCountingSpan implements Span {
        private final AtomicInteger openCount = new AtomicInteger();
        private final AtomicInteger closeCount = new AtomicInteger();

        boolean isScopeOpen() {
            return openCount.get() > closeCount.get();
        }

        @Override
        public void openScope() {
            openCount.incrementAndGet();
        }

        @Override
        public void closeScope() {
            closeCount.incrementAndGet();
        }

        @Override
        public void setQueryText(final BsonDocument commandDocument) {
        }

        @Override
        public void event(final String event) {
        }

        @Override
        public void error(final Throwable throwable) {
        }

        @Override
        public void end() {
        }

        @Override
        public TraceContext context() {
            return TraceContext.EMPTY;
        }

        @Override
        @Nullable
        public MongodbObservationContext getMongodbObservationContext() {
            return null;
        }

        @Override
        @Nullable
        public MongoNamespace getNamespace() {
            return null;
        }
    }
}
