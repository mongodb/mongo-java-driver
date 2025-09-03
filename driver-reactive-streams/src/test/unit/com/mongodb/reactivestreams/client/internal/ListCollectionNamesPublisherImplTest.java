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

package com.mongodb.reactivestreams.client.internal;

import com.mongodb.ReadPreference;
import com.mongodb.internal.operation.ListCollectionsOperation;
import com.mongodb.reactivestreams.client.ListCollectionNamesPublisher;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.Document;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import static com.mongodb.reactivestreams.client.MongoClients.getDefaultCodecRegistry;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;

final class ListCollectionNamesPublisherImplTest extends TestHelper {

    private static final String DATABASE_NAME = NAMESPACE.getDatabaseName();

    @SuppressWarnings("deprecation")
    @DisplayName("Should build the expected ListCollectionsOperation")
    @Test
    void shouldBuildTheExpectedOperation() {
        TestOperationExecutor executor = createOperationExecutor(asList(getBatchCursor(), getBatchCursor()));
        ListCollectionNamesPublisher publisher = new ListCollectionNamesPublisherImpl(
                new ListCollectionsPublisherImpl<>(null, createMongoOperationPublisher(executor)
                        .withDocumentClass(Document.class), true))
                .authorizedCollections(true);

        ListCollectionsOperation<Document> expectedOperation = new ListCollectionsOperation<>(DATABASE_NAME,
                                                                                              getDefaultCodecRegistry().get(Document.class))
                .batchSize(Integer.MAX_VALUE)
                .nameOnly(true)
                .authorizedCollections(true)
                .retryReads(true);

        // default input should be as expected
        Flux.from(publisher).blockFirst();

        assertOperationIsTheSameAs(expectedOperation, executor.getReadOperation());
        assertEquals(ReadPreference.primary(), executor.getReadPreference());

        // Should apply settings
        publisher
                .filter(new Document("filter", 1))
                .maxTime(10, SECONDS)
                .batchSize(100);

        expectedOperation = new ListCollectionsOperation<>(DATABASE_NAME,
                                                           getDefaultCodecRegistry().get(Document.class))
                .nameOnly(true)
                .authorizedCollections(true)
                .retryReads(true)
                .filter(new BsonDocument("filter", new BsonInt32(1)))
                .batchSize(100);

        Flux.from(publisher).blockFirst();
        assertOperationIsTheSameAs(expectedOperation, executor.getReadOperation());
        assertEquals(ReadPreference.primary(), executor.getReadPreference());
    }

}
