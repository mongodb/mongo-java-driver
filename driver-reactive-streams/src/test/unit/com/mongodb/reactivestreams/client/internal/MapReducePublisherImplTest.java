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
import com.mongodb.WriteConcern;
import com.mongodb.client.model.Sorts;
import com.mongodb.internal.operation.MapReduceStatistics;
import com.mongodb.internal.operation.MapReduceToCollectionOperation;
import com.mongodb.internal.operation.MapReduceWithInlineResultsOperation;
import com.mongodb.reactivestreams.client.MapReducePublisher;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonJavaScript;
import org.bson.Document;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import static com.mongodb.reactivestreams.client.MongoClients.getDefaultCodecRegistry;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings({"rawtypes"})
public class MapReducePublisherImplTest extends TestHelper {

    private static final String MAP_FUNCTION = "mapFunction(){}";
    private static final String REDUCE_FUNCTION = "reduceFunction(){}";
    private static final String FINALIZE_FUNCTION = "finalizeFunction(){}";

    @DisplayName("Should build the expected MapReduceWithInlineResultsOperation")
    @Test
    void shouldBuildTheExpectedMapReduceWithInlineResultsOperation() {
        configureBatchCursor();

        TestOperationExecutor executor = createOperationExecutor(asList(getBatchCursor(), getBatchCursor()));
        MapReducePublisher<Document> publisher =
                new MapReducePublisherImpl<>(null, createMongoOperationPublisher(executor), MAP_FUNCTION, REDUCE_FUNCTION);

        MapReduceWithInlineResultsOperation<Document> expectedOperation =
                new MapReduceWithInlineResultsOperation<>(NAMESPACE, new BsonJavaScript(MAP_FUNCTION), new BsonJavaScript(REDUCE_FUNCTION),
                                                          getDefaultCodecRegistry().get(Document.class)).verbose(true);

        // default input should be as expected
        Flux.from(publisher).blockFirst();

        MapReducePublisherImpl.WrappedMapReduceReadOperation operation =
                (MapReducePublisherImpl.WrappedMapReduceReadOperation) executor.getReadOperation();
        assertNotNull(operation);
        assertOperationIsTheSameAs(expectedOperation, operation.getOperation());
        assertEquals(ReadPreference.primary(), executor.getReadPreference());

        // Should apply settings
        publisher
                .batchSize(100)
                .bypassDocumentValidation(true)
                .collation(COLLATION)
                .filter(new Document("filter", 1))
                .finalizeFunction(FINALIZE_FUNCTION)
                .limit(999)
                .maxTime(10, SECONDS)
                .scope(new Document("scope", 1))
                .sort(Sorts.ascending("sort"))
                .verbose(false);

        expectedOperation
                .collation(COLLATION)
                .collation(COLLATION)
                .filter(BsonDocument.parse("{filter: 1}"))
                .finalizeFunction(new BsonJavaScript(FINALIZE_FUNCTION))
                .limit(999)
                .maxTime(10, SECONDS)
                .maxTime(10, SECONDS)
                .scope(new BsonDocument("scope", new BsonInt32(1)))
                .sort(new BsonDocument("sort", new BsonInt32(1)))
                .verbose(false);

        configureBatchCursor();
        Flux.from(publisher).blockFirst();
        operation = (MapReducePublisherImpl.WrappedMapReduceReadOperation) executor.getReadOperation();
        assertNotNull(operation);
        assertOperationIsTheSameAs(expectedOperation, operation.getOperation());
        assertEquals(ReadPreference.primary(), executor.getReadPreference());
    }

    @DisplayName("Should build the expected MapReduceToCollectionOperation")
    @Test
    void shouldBuildTheExpectedMapReduceToCollectionOperation() {
        MapReduceStatistics stats = Mockito.mock(MapReduceStatistics.class);

        TestOperationExecutor executor = createOperationExecutor(asList(stats, stats));
        MapReducePublisher<Document> publisher =
                new MapReducePublisherImpl<>(null, createMongoOperationPublisher(executor), MAP_FUNCTION, REDUCE_FUNCTION)
                        .collectionName(NAMESPACE.getCollectionName());

        MapReduceToCollectionOperation expectedOperation = new MapReduceToCollectionOperation(NAMESPACE,
                                                                                              new BsonJavaScript(MAP_FUNCTION),
                                                                                              new BsonJavaScript(REDUCE_FUNCTION),
                                                                                              NAMESPACE.getCollectionName(),
                                                                                              WriteConcern.ACKNOWLEDGED).verbose(true);

        // default input should be as expected
        Flux.from(publisher.toCollection()).blockFirst();
        assertOperationIsTheSameAs(expectedOperation, executor.getWriteOperation());

        // Should apply settings
        publisher
                .batchSize(100)
                .bypassDocumentValidation(true)
                .collation(COLLATION)
                .filter(new Document("filter", 1))
                .finalizeFunction(FINALIZE_FUNCTION)
                .limit(999)
                .maxTime(10, SECONDS)
                .scope(new Document("scope", 1))
                .sort(Sorts.ascending("sort"))
                .verbose(false);

        expectedOperation
                .collation(COLLATION)
                .bypassDocumentValidation(true)
                .filter(BsonDocument.parse("{filter: 1}"))
                .finalizeFunction(new BsonJavaScript(FINALIZE_FUNCTION))
                .limit(999)
                .maxTime(10, SECONDS)
                .maxTime(10, SECONDS)
                .scope(new BsonDocument("scope", new BsonInt32(1)))
                .sort(new BsonDocument("sort", new BsonInt32(1)))
                .verbose(false);

        Flux.from(publisher.toCollection()).blockFirst();
        assertOperationIsTheSameAs(expectedOperation, executor.getWriteOperation());
    }

    @DisplayName("Should handle error scenarios")
    @Test
    void shouldHandleErrorScenarios() {
        TestOperationExecutor executor = createOperationExecutor(asList(new MongoException("Failure"), null, null));

        // Operation fails
        MapReducePublisher<Document> publisher =
                new MapReducePublisherImpl<>(null, createMongoOperationPublisher(executor), MAP_FUNCTION, REDUCE_FUNCTION);
        assertThrows(MongoException.class, () -> Flux.from(publisher).blockFirst());

        // toCollection inline
        assertThrows(IllegalStateException.class, publisher::toCollection);

        // Missing Codec
        Publisher<Document> publisherMissingCodec =
                new MapReducePublisherImpl<>(null, createMongoOperationPublisher(executor)
                        .withCodecRegistry(BSON_CODEC_REGISTRY), MAP_FUNCTION, REDUCE_FUNCTION);
        assertThrows(CodecConfigurationException.class, () -> Flux.from(publisherMissingCodec).blockFirst());
    }
}
