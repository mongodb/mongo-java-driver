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
import com.mongodb.internal.operation.ListDatabasesOperation;
import com.mongodb.reactivestreams.client.ListDatabasesPublisher;
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

public class ListDatabasesPublisherImplTest extends TestHelper {

    @DisplayName("Should build the expected ListDatabasesOperation")
    @Test
    void shouldBuildTheExpectedOperation() {
        configureBatchCursor();

        TestOperationExecutor executor = createOperationExecutor(asList(getBatchCursor(), getBatchCursor()));
        ListDatabasesPublisher<Document> publisher = new ListDatabasesPublisherImpl<>(null, createMongoOperationPublisher(executor));

        ListDatabasesOperation<Document> expectedOperation = new ListDatabasesOperation<>(getDefaultCodecRegistry().get(Document.class))
                .retryReads(true);

        // default input should be as expected
        Flux.from(publisher).blockFirst();

        assertOperationIsTheSameAs(expectedOperation, executor.getReadOperation());
        assertEquals(ReadPreference.primary(), executor.getReadPreference());

        // Should apply settings
        publisher
                .authorizedDatabasesOnly(true)
                .filter(new Document("filter", 1))
                .maxTime(10, SECONDS)
                .batchSize(100);

        expectedOperation
                .authorizedDatabasesOnly(true)
                .filter(new BsonDocument("filter", new BsonInt32(1)))
                .maxTime(10, SECONDS);

        configureBatchCursor();
        Flux.from(publisher).blockFirst();
        assertOperationIsTheSameAs(expectedOperation, executor.getReadOperation());
        assertEquals(ReadPreference.primary(), executor.getReadPreference());
    }

}
