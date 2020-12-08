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

package com.mongodb.reactivestreams.client.internal.crypt;

import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.reactivestreams.client.MongoClient;
import org.bson.BsonDocument;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.Closeable;
import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;

class KeyRetriever implements Closeable {
    private final MongoClient client;
    private final boolean ownsClient;
    private final MongoNamespace namespace;

    KeyRetriever(final MongoClient client, final boolean ownsClient, final MongoNamespace namespace) {
        this.client = notNull("client", client);
        this.ownsClient = ownsClient;
        this.namespace = notNull("namespace", namespace);
    }

    public Mono<List<BsonDocument>> find(final BsonDocument keyFilter) {
        return Flux.from(
                client.getDatabase(namespace.getDatabaseName()).getCollection(namespace.getCollectionName(), BsonDocument.class)
                        .withReadConcern(ReadConcern.MAJORITY)
                        .find(keyFilter)
        ).collectList();
    }

    @Override
    public void close() {
        if (ownsClient) {
            client.close();
        }
    }
}
