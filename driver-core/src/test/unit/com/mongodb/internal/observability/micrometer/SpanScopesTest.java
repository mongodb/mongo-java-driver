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

class SpanScopesTest {

    @Test
    void runsActionDirectlyWhenSpanIsNull() {
        List<String> calls = new ArrayList<>();
        SpanScopes.runInScope(null, () -> calls.add("action"));
        assertEquals(asList("action"), calls);
    }

    @Test
    void opensScopeBeforeActionAndClosesAfter() {
        List<String> calls = new ArrayList<>();
        SpanScopes.runInScope(new RecordingSpan(calls), () -> calls.add("action"));
        assertEquals(asList("open", "action", "close"), calls);
    }

    @Test
    void closesScopeWhenActionThrows() {
        List<String> calls = new ArrayList<>();
        RuntimeException failure = new RuntimeException("appender failed");
        RuntimeException thrown = assertThrows(RuntimeException.class,
                () -> SpanScopes.runInScope(new RecordingSpan(calls), () -> { throw failure; }));
        assertEquals(failure, thrown);
        assertEquals(asList("open", "close"), calls);
    }

    static final class RecordingSpan implements Span {
        private final List<String> calls;

        RecordingSpan(final List<String> calls) {
            this.calls = calls;
        }

        @Override public void openScope() { calls.add("open"); }
        @Override public void closeScope() { calls.add("close"); }
        @Override public void setQueryText(final BsonDocument commandDocument) { }
        @Override public void event(final String event) { }
        @Override public void error(final Throwable throwable) { }
        @Override public void end() { }
        @Override public TraceContext context() { return TraceContext.EMPTY; }
        @Override @Nullable public MongodbObservationContext getMongodbObservationContext() { return null; }
        @Override @Nullable public MongoNamespace getNamespace() { return null; }
    }
}
