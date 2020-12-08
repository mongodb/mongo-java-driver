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

import com.mongodb.MongoNamespace;
import com.mongodb.ReadPreference;
import com.mongodb.internal.operation.ListIndexesOperation;
import com.mongodb.reactivestreams.client.ListIndexesPublisher;
import org.bson.Document;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import static com.mongodb.reactivestreams.client.MongoClients.getDefaultCodecRegistry;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ListIndexesPublisherImplTest extends TestHelper {

    private static final MongoNamespace NAMESPACE = new MongoNamespace("db", "coll");

    @DisplayName("Should build the expected ListIndexesOperation")
    @Test
    void shouldBuildTheExpectedOperation() {
        configureBatchCursor();

        TestOperationExecutor executor = createOperationExecutor(asList(getBatchCursor(), getBatchCursor()));
        ListIndexesPublisher<Document> publisher = new ListIndexesPublisherImpl<>(null, createMongoOperationPublisher(executor));

        ListIndexesOperation<Document> expectedOperation =
                new ListIndexesOperation<>(NAMESPACE, getDefaultCodecRegistry().get(Document.class))
                        .retryReads(true);

        // default input should be as expected
        Flux.from(publisher).blockFirst();

        assertOperationIsTheSameAs(expectedOperation, executor.getReadOperation());
        assertEquals(ReadPreference.primary(), executor.getReadPreference());

        // Should apply settings
        publisher
                .batchSize(100)
                .maxTime(10, SECONDS);

        expectedOperation
                .batchSize(100)
                .maxTime(10, SECONDS);

        configureBatchCursor();
        Flux.from(publisher).blockFirst();
        assertOperationIsTheSameAs(expectedOperation, executor.getReadOperation());
        assertEquals(ReadPreference.primary(), executor.getReadPreference());
    }

}
