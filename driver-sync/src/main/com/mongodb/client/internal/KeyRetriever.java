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
import static com.mongodb.client.internal.TimeoutHelper.databaseWithTimeout;

class KeyRetriever {
    private static final String TIMEOUT_ERROR_MESSAGE = "Key retrieval exceeded the timeout limit.";
    private final MongoClient client;
    private final MongoNamespace namespace;

    KeyRetriever(final MongoClient client, final MongoNamespace namespace) {
        this.client = notNull("client", client);
        this.namespace = notNull("namespace", namespace);
    }

    public List<BsonDocument> find(final BsonDocument keyFilter, @Nullable final Timeout operationTimeout) {
        MongoCollection<BsonDocument> collection = client.getDatabase(namespace.getDatabaseName())
                .getCollection(namespace.getCollectionName(), BsonDocument.class);

        return collectionWithTimeout(collection, TIMEOUT_ERROR_MESSAGE, operationTimeout)
                .withReadConcern(ReadConcern.MAJORITY)
                .find(keyFilter).into(new ArrayList<>());
    }
}
