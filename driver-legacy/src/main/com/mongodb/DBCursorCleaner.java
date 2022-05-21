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

/**
 * A cleaner for abandoned {@link DBCursor} instances.
 *
 * @see DBCursor
 */
abstract class DBCursorCleaner {
    // Should be true on Java 9+
    private static final boolean CLEANER_IS_AVAILABLE;

    static {
        boolean cleanerIsAvailable = false;
        try {
            Class.forName("java.lang.ref.Cleaner");
            cleanerIsAvailable = true;
        } catch (ClassNotFoundException ignored) {
        }
        CLEANER_IS_AVAILABLE = cleanerIsAvailable;
    }

    /**
     * Create a new instance.
     *
     * <p>
     * The implementation of this method ensures that a {@code java.lang.ref.Cleaner}-based implementation is used when
     * the runtime is Java 9+.  Otherwise a {@link Object#finalize}-based implementation is used.
     * </p>
     *
     * @param mongoClient the client from which the {@link DBCursor} came from
     * @param namespace the namespace of the cursor
     * @param serverCursor the server cursor
     * @return the cleaner
     */
    @SuppressWarnings("deprecation")
    static DBCursorCleaner create(final MongoClient mongoClient, final MongoNamespace namespace,
                           final ServerCursor serverCursor) {
        if (CLEANER_IS_AVAILABLE) {
            return new Java9DBCursorCleaner(mongoClient, namespace, serverCursor);
        } else {
            return new Java8DBCursorCleaner(mongoClient, namespace, serverCursor);
        }
    }

    /**
     * {@link DBCursor} should call this method when the cursor has been exhausted and/or explicitly closed by the
     * application.
     */
    abstract void clearCursor();
}
