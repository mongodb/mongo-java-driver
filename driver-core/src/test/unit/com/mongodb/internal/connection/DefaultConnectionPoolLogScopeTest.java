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

import com.mongodb.MongoNamespace;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.connection.ServerId;
import com.mongodb.internal.inject.SameObjectProvider;
import com.mongodb.internal.logging.LogMessage;
import com.mongodb.internal.observability.micrometer.Span;
import com.mongodb.internal.observability.micrometer.SpanScope;
import com.mongodb.internal.observability.micrometer.TraceContext;
import com.mongodb.lang.Nullable;
import com.mongodb.logging.TestLoggingInterceptor;
import com.mongodb.observability.micrometer.MongodbObservationContext;
import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mongodb.ClusterFixture.OPERATION_CONTEXT_FACTORY;
import static com.mongodb.ClusterFixture.createOperationContext;
import static com.mongodb.internal.logging.LogMessage.Component.CONNECTION;
import static com.mongodb.internal.logging.LogMessage.Level.DEBUG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Verifies that the "Connection checked in" log message is emitted while the span of the most recent checkout is
 * open, so an MDC bridge reading the current span at log time attributes it to the right operation. A real pool
 * with a test connection factory is driven through two checkouts of the same connection, with fake spans that
 * count scope opens and closes. This covers connection reuse, which the end-to-end prose tests do not.
 */
final class DefaultConnectionPoolLogScopeTest {

    @Test
    void checkedInMessageUsesTheSpanOfTheMostRecentCheckout() {
        String applicationName = "DefaultConnectionPoolLogScopeTest2";
        ServerId serverId = new ServerId(new ClusterId(applicationName), new ServerAddress());
        ScopeCountingSpan firstSpan = new ScopeCountingSpan();
        ScopeCountingSpan secondSpan = new ScopeCountingSpan();
        OperationContext first = createOperationContext();
        first.setTracingSpan(firstSpan);
        OperationContext second = createOperationContext();
        second.setTracingSpan(secondSpan);
        DefaultConnectionPool pool = new DefaultConnectionPool(serverId, new TestInternalConnectionFactory(),
                ConnectionPoolSettings.builder().maxSize(1).build(),
                SameObjectProvider.initialized(mock(SdamServerDescriptionManager.class)), OPERATION_CONTEXT_FACTORY);
        pool.ready();

        try (ScopeRecordingInterceptor interceptor = new ScopeRecordingInterceptor(applicationName, secondSpan)) {
            pool.get(first).close();
            interceptor.scopeOpenAtMessage.clear();
            int firstSpanOpensBeforeSecondCheckout = firstSpan.openCount.get();
            pool.get(second).close();

            interceptor.assertEmittedInsideScope("Connection checked in"); // must use the second checkout's span
            assertEquals(firstSpanOpensBeforeSecondCheckout, firstSpan.openCount.get(),
                    "the first checkout's span must not be re-entered by the second checkout");
            assertEquals(0, firstSpan.openCount.get() - firstSpan.closeCount.get());
        } finally {
            pool.close();
        }
    }

    private static final class ScopeRecordingInterceptor extends TestLoggingInterceptor {
        private final ScopeCountingSpan span;
        private final Map<String, Boolean> scopeOpenAtMessage = new ConcurrentHashMap<>();

        ScopeRecordingInterceptor(final String applicationName, final ScopeCountingSpan span) {
            super(applicationName, new LoggingFilter(Collections.singletonMap(CONNECTION, DEBUG)));
            this.span = span;
        }

        @Override
        public synchronized void intercept(final LogMessage message) {
            super.intercept(message);
            scopeOpenAtMessage.put(message.getMessageId(), span.isScopeOpen());
        }

        void assertEmittedInsideScope(final String messageId) {
            Boolean scopeOpen = scopeOpenAtMessage.get(messageId);
            assertNotNull(scopeOpen, messageId + " was not emitted");
            assertTrue(scopeOpen, messageId + " must be emitted inside the span scope");
        }
    }

    static final class ScopeCountingSpan implements Span {
        private final AtomicInteger openCount = new AtomicInteger();
        private final AtomicInteger closeCount = new AtomicInteger();

        boolean isScopeOpen() {
            return openCount.get() > closeCount.get();
        }

        @Override
        public SpanScope openScope() {
            openCount.incrementAndGet();
            return closeCount::incrementAndGet;
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
