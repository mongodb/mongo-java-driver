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

import com.mongodb.MongoException;
import com.mongodb.ReadPreference;
import com.mongodb.internal.operation.DistinctOperation;
import com.mongodb.reactivestreams.client.DistinctPublisher;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import static com.mongodb.reactivestreams.client.MongoClients.getDefaultCodecRegistry;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DistinctPublisherImplTest extends TestHelper {

    @SuppressWarnings("deprecation")
    @DisplayName("Should build the expected DistinctOperation")
    @Test
    void shouldBuildTheExpectedOperation() {
        String fieldName = "fieldName";
        TestOperationExecutor executor = createOperationExecutor(asList(getBatchCursor(), getBatchCursor()));
        DistinctPublisher<Document> publisher =
                new DistinctPublisherImpl<>(null, createMongoOperationPublisher(executor), fieldName, new Document());

        DistinctOperation<Document> expectedOperation = new DistinctOperation<>(CSOT_NO_TIMEOUT, NAMESPACE, fieldName,
                                                                                getDefaultCodecRegistry().get(Document.class))
                .retryReads(true).filter(new BsonDocument());

        // default input should be as expected
        Flux.from(publisher).blockFirst();

        assertOperationIsTheSameAs(expectedOperation, executor.getReadOperation());
        assertEquals(ReadPreference.primary(), executor.getReadPreference());

        // Should apply settings
        BsonDocument filter = BsonDocument.parse("{a: 1}");
        publisher
                .batchSize(100)
                .collation(COLLATION)
                .maxTime(99, MILLISECONDS)
                .filter(filter);

        expectedOperation =  new DistinctOperation<>(CSOT_MAX_AWAIT_TIME, NAMESPACE, fieldName,
                getDefaultCodecRegistry().get(Document.class))
                .retryReads(true)
                .collation(COLLATION)
                .filter(filter);

        configureBatchCursor();
        Flux.from(publisher).blockFirst();
        assertEquals(ReadPreference.primary(), executor.getReadPreference());
        assertOperationIsTheSameAs(expectedOperation, executor.getReadOperation());
    }


    @DisplayName("Should handle error scenarios")
    @Test
    void shouldHandleErrorScenarios() {
        TestOperationExecutor executor = createOperationExecutor(singletonList(new MongoException("Failure")));

        // Operation fails
        Publisher<Document> publisher =
                new DistinctPublisherImpl<>(null, createMongoOperationPublisher(executor), "fieldName", new Document());
        assertThrows(MongoException.class, () -> Flux.from(publisher).blockFirst());

        // Missing Codec
        TestOperationExecutor missingCodecExecutor = createOperationExecutor(singletonList(getBatchCursor()));
        Publisher<Document> publisherMissingCodec =
                new DistinctPublisherImpl<>(null, createMongoOperationPublisher(missingCodecExecutor)
                        .withCodecRegistry(BSON_CODEC_REGISTRY), "fieldName", new Document());
        assertThrows(CodecConfigurationException.class, () -> {
            Flux.from(publisherMissingCodec).blockFirst();
            missingCodecExecutor.getReadOperation();
        });
    }

}
