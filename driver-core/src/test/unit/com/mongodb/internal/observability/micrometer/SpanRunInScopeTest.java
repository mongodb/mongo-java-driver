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

package com.mongodb.internal.observability.micrometer;

import com.mongodb.MongoNamespace;
import com.mongodb.lang.Nullable;
import com.mongodb.observability.micrometer.MongodbObservationContext;
import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Verifies the contract of {@link Span#runInScope}, the helper every log site uses to make a span current for
 * the duration of a log call: a null span runs the action with no scope at all; otherwise the scope is opened
 * before the action and closed after it, also when the action throws (the action's exception propagates
 * unchanged); and nested calls on the same span open and close in LIFO order. A recording fake stands in for
 * the span, so only the call sequence is checked here; Micrometer behaviour is covered by
 * {@link MicrometerSpanScopeTest}.
 */
class SpanRunInScopeTest {

    @Test
    void runsActionDirectlyWhenSpanIsNull() {
        List<String> calls = new ArrayList<>();
        Span.runInScope(null, () -> calls.add("action"));
        assertEquals(asList("action"), calls);
    }

    @Test
    void opensScopeBeforeActionAndClosesAfter() {
        List<String> calls = new ArrayList<>();
        Span.runInScope(new RecordingSpan(calls), () -> calls.add("action"));
        assertEquals(asList("open", "action", "close"), calls);
    }

    @Test
    void closesScopeWhenActionThrows() {
        List<String> calls = new ArrayList<>();
        RuntimeException failure = new RuntimeException("appender failed");
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> Span.runInScope(new RecordingSpan(calls), () -> {
            throw failure;
        }));
        assertEquals(failure, thrown);
        assertEquals(asList("open", "close"), calls);
    }

    @Test
    void nestsScopes() {
        List<String> calls = new ArrayList<>();
        RecordingSpan span = new RecordingSpan(calls);
        Span.runInScope(span, () -> Span.runInScope(span, () -> calls.add("action")));
        assertEquals(asList("open", "open", "action", "close", "close"), calls);
    }

    static final class RecordingSpan implements Span {
        private final List<String> calls;

        RecordingSpan(final List<String> calls) {
            this.calls = calls;
        }

        @Override
        public void openScope() {
            calls.add("open");
        }

        @Override
        public void closeScope() {
            calls.add("close");
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
