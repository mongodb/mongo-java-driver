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

package com.mongodb.client.internal;

import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.internal.time.Timeout;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.client.internal.TimeoutHelper.collectionWithTimeout;

/**
 * A utility class for retrieving keys from a MongoDB collection with timeout and read concern handling.
 * This implementation ensures consistent reads by using MAJORITY read concern and supports
 * operation timeouts to prevent long-running queries.
 *
 * <p>Key features:</p>
 * <ul>
 *     <li>Configurable operation timeouts to prevent resource exhaustion</li>
 *     <li>Uses ReadConcern.MAJORITY to ensure consistent reads across replicas</li>
 *     <li>Supports filtered key retrieval through query documents</li>
 * </ul>
 *
 * <p>Performance considerations:</p>
 * <ul>
 *     <li>MAJORITY read concern may increase latency due to replica set coordination</li>
 *     <li>Timeout handling prevents unbounded query execution</li>
 *     <li>Results are collected into memory, so consider memory usage for large result sets</li>
 * </ul>
 *
 * <p>Thread safety: This class is thread-safe as it maintains no mutable state
 * and delegates to thread-safe MongoDB driver components.</p>
 */
class KeyRetriever {
    private static final String TIMEOUT_ERROR_MESSAGE = "Key retrieval exceeded the timeout limit.";
    private final MongoClient client;
    private final MongoNamespace namespace;

    /**
     * Creates a new key retriever for the specified client and namespace.
     *
     * @param client the MongoDB client to use for operations
     * @param namespace the namespace (database.collection) to retrieve keys from
     * @throws IllegalArgumentException if client or namespace is null
     */
    KeyRetriever(final MongoClient client, final MongoNamespace namespace) {
        this.client = notNull("client", client);
        this.namespace = notNull("namespace", namespace);
    }

    /**
     * Finds keys in the collection that match the specified filter.
     * Uses ReadConcern.MAJORITY to ensure consistent reads and supports operation timeout.
     *
     * <p>The operation will:</p>
     * <ul>
     *     <li>Apply the specified timeout if provided</li>
     *     <li>Use MAJORITY read concern for consistency</li>
     *     <li>Collect all matching documents into memory</li>
     * </ul>
     *
     * @param keyFilter the filter to apply when finding keys
     * @param operationTimeout optional timeout for the operation
     * @return list of documents matching the filter
     * @throws com.mongodb.MongoTimeoutException if the operation exceeds the timeout
     */
    public List<BsonDocument> find(final BsonDocument keyFilter, @Nullable final Timeout operationTimeout) {
        MongoCollection<BsonDocument> collection = client.getDatabase(namespace.getDatabaseName())
                .getCollection(namespace.getCollectionName(), BsonDocument.class);

        return collectionWithTimeout(collection, TIMEOUT_ERROR_MESSAGE, operationTimeout)
                .withReadConcern(ReadConcern.MAJORITY)
                .find(keyFilter).into(new ArrayList<>());
    }
}
