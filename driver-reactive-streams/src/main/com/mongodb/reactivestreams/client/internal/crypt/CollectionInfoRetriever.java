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

import com.mongodb.internal.time.Timeout;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.MongoClient;
import org.bson.BsonDocument;
import reactor.core.publisher.Flux;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.reactivestreams.client.internal.TimeoutHelper.databaseWithTimeoutDeferred;

class CollectionInfoRetriever {

    private static final String TIMEOUT_ERROR_MESSAGE = "Collection information retrieval exceeded the timeout limit.";

    private final MongoClient client;

    CollectionInfoRetriever(final MongoClient client) {
        this.client = notNull("client", client);
    }

    public Flux<BsonDocument> filter(final String databaseName, final BsonDocument filter, @Nullable final Timeout operationTimeout) {
        return databaseWithTimeoutDeferred(client.getDatabase(databaseName), TIMEOUT_ERROR_MESSAGE, operationTimeout)
                .flatMapMany(database -> Flux.from(database.listCollections(BsonDocument.class).filter(filter)));
    }
}
