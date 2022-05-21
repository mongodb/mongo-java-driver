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

package com.mongodb;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static com.mongodb.assertions.Assertions.assertNotNull;

/**
 * A {@code java.lang.ref.Cleaner}-based implementation of {@link DBCursorCleaner}.  The implementation
 * is reflection-based so that it will compile with Java 8 even though  {@code java.lang.ref.Cleaner} was introduced in
 * Java 9.  {@link DBCursorCleaner#create(MongoClient, MongoNamespace, ServerCursor)} is responsible for ensuring that
 * this class is only used if the {@code java.lang.ref.Cleaner} class is available (i.e. the runtime is Java 9+).
 */
final class Java9DBCursorCleaner extends DBCursorCleaner {
    // Actual type is java.lang.ref.Cleaner
    private static final Object CLEANER;
    // Actual method is Cleaner#register(Object, Runnable)
    private static final Method REGISTER_METHOD;
    // Actual method is Cleanable#clean
    private static final Method CLEAN_METHOD;

    static {
        try {
            Class<?> cleanerClass = Class.forName("java.lang.ref.Cleaner");
            CLEANER = cleanerClass.getMethod("create").invoke(null);
            REGISTER_METHOD = cleanerClass.getMethod("register", Object.class, Runnable.class);
            CLEAN_METHOD = Class.forName("java.lang.ref.Cleaner$Cleanable").getMethod("clean");
        } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException | ClassNotFoundException e) {
            throw new MongoInternalException("Unexpected exception", e);
        }
    }

    private final CleanerState cleanerState;
    // Actual type is java.lang.ref.Cleaner$Cleanable
    private final Object cleanable;

    Java9DBCursorCleaner(final MongoClient mongoClient, final MongoNamespace namespace,
                         final ServerCursor serverCursor) {
        cleanerState = new CleanerState(mongoClient, namespace, serverCursor);
        try {
            cleanable = REGISTER_METHOD.invoke(CLEANER, this, cleanerState);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new MongoInternalException("Unexpected exception", e);
        }
    }

    @Override
    void clearCursor() {
        cleanerState.clear();
        try {
            CLEAN_METHOD.invoke(cleanable);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new MongoInternalException("Unexpected exception", e);
        }
    }

    private static class CleanerState implements Runnable {
        private final MongoClient mongoClient;
        private final MongoNamespace namespace;
        private volatile ServerCursor serverCursor;

        CleanerState(final MongoClient mongoClient, final MongoNamespace namespace, final ServerCursor serverCursor) {
            this.mongoClient = assertNotNull(mongoClient);
            this.namespace = assertNotNull(namespace);
            this.serverCursor = assertNotNull(serverCursor);
        }

        public void run() {
            if (serverCursor != null) {
                mongoClient.addOrphanedCursor(serverCursor, namespace);
            }
        }

        public void clear() {
            serverCursor = null;
        }
    }
}
