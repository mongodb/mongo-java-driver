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

import static com.mongodb.assertions.Assertions.assertNotNull;

/**
 * A {@link Object#finalize()}-based implementation of {@link DBCursorCleaner}.
 * {@link DBCursorCleaner#create(MongoClient, MongoNamespace, ServerCursor)} is responsible for ensuring
 * that this class is only used if the {@code java.lang.ref.Cleaner} class is not available
 * (i.e. the runtime is Java 8).
 */
final class Java8DBCursorCleaner extends DBCursorCleaner {
    private final MongoClient mongoClient;
    private final MongoNamespace namespace;
    private ServerCursor serverCursor;

    Java8DBCursorCleaner(final MongoClient mongoClient, final MongoNamespace namespace,
                         final ServerCursor serverCursor) {
        this.mongoClient = assertNotNull(mongoClient);
        this.namespace = assertNotNull(namespace);
        this.serverCursor = assertNotNull(serverCursor);
    }

    @Override
    void clearCursor() {
       serverCursor = null;
    }

    @Override
    @SuppressWarnings("deprecation")
    protected void finalize() {
        if (serverCursor != null) {
            mongoClient.addOrphanedCursor(serverCursor, namespace);
        }
    }
}
