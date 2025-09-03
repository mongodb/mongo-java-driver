/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mongodb.reactivestreams.client.syncadapter;

import com.mongodb.ContextProvider;
import com.mongodb.RequestContext;
import com.mongodb.reactivestreams.client.ReactiveContextProvider;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import reactor.core.CoreSubscriber;
import reactor.util.context.Context;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

public final class ContextHelper {
    static final ResettableRequestContext REQUEST_CONTEXT = new ResettableRequestContext();
    static final Context CONTEXT = Context.of("A", "B");

    public static final ContextProvider CONTEXT_PROVIDER = (ReactiveContextProvider) subscriber -> {
        if (subscriber instanceof CoreSubscriber<?>) {
            ((CoreSubscriber<?>) subscriber)
                    .currentContext()
                    .stream()
                    .forEach(e -> REQUEST_CONTEXT.put(e.getKey(), e.getValue()));
        }
        return REQUEST_CONTEXT;
    };

    public static void assertContextPassedThrough() {
        boolean contextWasSet = REQUEST_CONTEXT.size() > 0;
        REQUEST_CONTEXT.reset();
        assertTrue(contextWasSet, "Test failed to pass through the context as expected");
    }

    public static void assertContextPassedThrough(final BsonDocument unifiedTestDefinition) {
        boolean shouldBeSuccessful = unifiedTestDefinition.getArray("operations", new BsonArray())
                .stream()
                .anyMatch(v -> {
                    BsonValue result = v.asDocument().get("result", new BsonDocument());
                    return !result.isDocument() || result.asDocument().containsKey("errorContains");
                });
        if (shouldBeSuccessful) {
            assertContextPassedThrough();
        }
    }


    @SuppressWarnings("unchecked")
    static class ResettableRequestContext implements RequestContext {

        private final Map<Object, Object> context = new ConcurrentHashMap<>();

        @Override
        public <T> T get(final Object key) {
            return (T) context.get(key);
        }

        @Override
        public <T> T get(final Class<T> key) {
            return (T) context.get(key);
        }

        @Override
        public <T> T getOrDefault(final Object key, final T defaultValue) {
            return (T) context.getOrDefault(key, defaultValue);
        }

        @Override
        public <T> Optional<T> getOrEmpty(final Object key) {
            return (Optional<T>) context.compute(key, (k, v) -> v == null ? Optional.empty() : Optional.of(((T) v)));
        }

        @Override
        public boolean hasKey(final Object key) {
            return context.containsKey(key);
        }

        @Override
        public boolean isEmpty() {
            return context.isEmpty();
        }

        @Override
        public void put(final Object key, final Object value) {
            context.put(key, value);
        }

        @Override
        public void putNonNull(final Object key, final Object valueOrNull) {
            context.put(key, valueOrNull);
        }

        @Override
        public void delete(final Object key) {
            context.remove(key);
        }

        @Override
        public int size() {
            return context.size();
        }

        @Override
        public Stream<Map.Entry<Object, Object>> stream() {
            return context.entrySet().stream();
        }

        void reset() {
            context.clear();
        }
    }

    private ContextHelper() {
    }
}
