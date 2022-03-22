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
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.internal.client.model.AggregationLevel;
import com.mongodb.internal.operation.AggregateOperation;
import com.mongodb.internal.operation.AggregateToCollectionOperation;
import com.mongodb.internal.operation.FindOperation;
import com.mongodb.reactivestreams.client.AggregatePublisher;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.util.List;

import static com.mongodb.reactivestreams.client.MongoClients.getDefaultCodecRegistry;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings({"rawtypes"})
public class AggregatePublisherImplTest extends TestHelper {

    @DisplayName("Should build the expected AggregateOperation")
    @Test
    void shouldBuildTheExpectedOperation() {
        List<BsonDocument> pipeline = singletonList(BsonDocument.parse("{'$match': 1}"));

        TestOperationExecutor executor = createOperationExecutor(asList(getBatchCursor(), getBatchCursor()));
        AggregatePublisher<Document> publisher =
                new AggregatePublisherImpl<>(null, createMongoOperationPublisher(executor), pipeline, AggregationLevel.COLLECTION);

        AggregateOperation<Document> expectedOperation = new AggregateOperation<>(NAMESPACE, pipeline,
                                                                                  getDefaultCodecRegistry().get(Document.class))
                .batchSize(Integer.MAX_VALUE)
                .retryReads(true);

        // default input should be as expected
        Flux.from(publisher).blockFirst();

        assertOperationIsTheSameAs(expectedOperation, executor.getReadOperation());
        assertEquals(ReadPreference.primary(), executor.getReadPreference());

        // Should apply settings
        publisher
                .allowDiskUse(true)
                .batchSize(100)
                .bypassDocumentValidation(true) // Ignored
                .collation(COLLATION)
                .comment("my comment")
                .hint(BsonDocument.parse("{a: 1}"))
                .maxAwaitTime(20, SECONDS)
                .maxTime(10, SECONDS);

        expectedOperation
                .allowDiskUse(true)
                .batchSize(100)
                .collation(COLLATION)
                .comment(new BsonString("my comment"))
                .hint(BsonDocument.parse("{a: 1}"))
                .maxAwaitTime(20, SECONDS)
                .maxTime(10, SECONDS);

        Flux.from(publisher).blockFirst();
        assertOperationIsTheSameAs(expectedOperation, executor.getReadOperation());
        assertEquals(ReadPreference.primary(), executor.getReadPreference());
    }

    @DisplayName("Should build the expected AggregateOperation for hint string")
    @Test
    void shouldBuildTheExpectedOperationForHintString() {
        List<BsonDocument> pipeline = singletonList(BsonDocument.parse("{'$match': 1}"));

        TestOperationExecutor executor = createOperationExecutor(asList(getBatchCursor(), getBatchCursor()));
        AggregatePublisher<Document> publisher =
                new AggregatePublisherImpl<>(null, createMongoOperationPublisher(executor), pipeline, AggregationLevel.COLLECTION);

        AggregateOperation<Document> expectedOperation = new AggregateOperation<>(NAMESPACE, pipeline,
                getDefaultCodecRegistry().get(Document.class))
                .batchSize(Integer.MAX_VALUE)
                .retryReads(true);

        publisher
                .hintString("x_1");

        expectedOperation
                .hint(new BsonString("x_1"));

        Flux.from(publisher).blockFirst();
        assertOperationIsTheSameAs(expectedOperation, executor.getReadOperation());
    }

    @DisplayName("Should build the expected AggregateOperation when both hint and hintString are set")
    @Test
    void shouldBuildTheExpectedOperationForHintPlusHintString() {
        List<BsonDocument> pipeline = singletonList(BsonDocument.parse("{'$match': 1}"));

        TestOperationExecutor executor = createOperationExecutor(asList(getBatchCursor(), getBatchCursor()));
        AggregatePublisher<Document> publisher =
                new AggregatePublisherImpl<>(null, createMongoOperationPublisher(executor), pipeline, AggregationLevel.COLLECTION);

        AggregateOperation<Document> expectedOperation = new AggregateOperation<>(NAMESPACE, pipeline,
                getDefaultCodecRegistry().get(Document.class))
                .batchSize(Integer.MAX_VALUE)
                .retryReads(true);

        publisher
                .hint(new Document("x", 1))
                .hintString("x_1");

        expectedOperation
                .hint(new BsonDocument("x", new BsonInt32(1)));

        Flux.from(publisher).blockFirst();
        assertOperationIsTheSameAs(expectedOperation, executor.getReadOperation());
    }

    @DisplayName("Should build the expected AggregateOperation for $out")
    @Test
    void shouldBuildTheExpectedOperationsForDollarOut() {
        String collectionName = "collectionName";
        List<BsonDocument> pipeline = asList(BsonDocument.parse("{'$match': 1}"),
                                             BsonDocument.parse(format("{'$out': '%s'}", collectionName)));
        MongoNamespace collectionNamespace = new MongoNamespace(NAMESPACE.getDatabaseName(), collectionName);

        TestOperationExecutor executor = createOperationExecutor(asList(getBatchCursor(), getBatchCursor(), getBatchCursor(), null));
        AggregatePublisher<Document> publisher =
                new AggregatePublisherImpl<>(null, createMongoOperationPublisher(executor), pipeline, AggregationLevel.COLLECTION);

        AggregateToCollectionOperation expectedOperation = new AggregateToCollectionOperation(NAMESPACE, pipeline,
                                                                                              ReadConcern.DEFAULT,
                                                                                              WriteConcern.ACKNOWLEDGED);

        // default input should be as expected
        Flux.from(publisher).blockFirst();

        VoidReadOperationThenCursorReadOperation operation = (VoidReadOperationThenCursorReadOperation) executor.getReadOperation();
        assertEquals(ReadPreference.primary(), executor.getReadPreference());
        assertOperationIsTheSameAs(expectedOperation, operation.getReadOperation());

        // Should apply settings
        publisher
                .allowDiskUse(true)
                .batchSize(100) // Used in Find
                .bypassDocumentValidation(true)
                .collation(COLLATION)
                .comment("my comment")
                .hint(BsonDocument.parse("{a: 1}"))
                .maxAwaitTime(20, SECONDS) // Ignored on $out
                .maxTime(10, SECONDS);

        expectedOperation
                .allowDiskUse(true)
                .bypassDocumentValidation(true)
                .collation(COLLATION)
                .comment(new BsonString("my comment"))
                .hint(BsonDocument.parse("{a: 1}"))
                .maxTime(10, SECONDS);

        Flux.from(publisher).blockFirst();
        assertEquals(ReadPreference.primary(), executor.getReadPreference());
        operation = (VoidReadOperationThenCursorReadOperation) executor.getReadOperation();
        assertOperationIsTheSameAs(expectedOperation, operation.getReadOperation());

        FindOperation<Document> expectedFindOperation =
                new FindOperation<>(collectionNamespace, getDefaultCodecRegistry().get(Document.class))
                        .batchSize(100)
                        .collation(COLLATION)
                        .filter(new BsonDocument())
                        .maxAwaitTime(0, SECONDS)
                        .maxTime(0, SECONDS)
                        .comment(new BsonString("my comment"))
                        .retryReads(true);

        assertOperationIsTheSameAs(expectedFindOperation, operation.getCursorReadOperation());

        // Should handle database level aggregations
        publisher = new AggregatePublisherImpl<>(null, createMongoOperationPublisher(executor), pipeline, AggregationLevel.DATABASE);

        expectedOperation = new AggregateToCollectionOperation(NAMESPACE, pipeline, ReadConcern.DEFAULT, WriteConcern.ACKNOWLEDGED);

        Flux.from(publisher).blockFirst();
        operation = (VoidReadOperationThenCursorReadOperation) executor.getReadOperation();
        assertEquals(ReadPreference.primary(), executor.getReadPreference());
        assertOperationIsTheSameAs(expectedOperation, operation.getReadOperation());

        // Should handle toCollection
        publisher = new AggregatePublisherImpl<>(null, createMongoOperationPublisher(executor), pipeline, AggregationLevel.COLLECTION);

        expectedOperation = new AggregateToCollectionOperation(NAMESPACE, pipeline, ReadConcern.DEFAULT, WriteConcern.ACKNOWLEDGED);

        // default input should be as expected
        Flux.from(publisher.toCollection()).blockFirst();
        assertOperationIsTheSameAs(expectedOperation, executor.getReadOperation());
    }

    @DisplayName("Should build the expected AggregateOperation for $out with hint string")
    @Test
    void shouldBuildTheExpectedOperationsForDollarOutWithHintString() {
        String collectionName = "collectionName";
        List<BsonDocument> pipeline = asList(BsonDocument.parse("{'$match': 1}"),
                BsonDocument.parse(format("{'$out': '%s'}", collectionName)));
        MongoNamespace collectionNamespace = new MongoNamespace(NAMESPACE.getDatabaseName(), collectionName);

        TestOperationExecutor executor = createOperationExecutor(asList(getBatchCursor(), getBatchCursor(), getBatchCursor(), null));
        AggregatePublisher<Document> publisher =
                new AggregatePublisherImpl<>(null, createMongoOperationPublisher(executor), pipeline, AggregationLevel.COLLECTION);

        AggregateToCollectionOperation expectedOperation = new AggregateToCollectionOperation(NAMESPACE, pipeline,
                ReadConcern.DEFAULT,
                WriteConcern.ACKNOWLEDGED);

        publisher
                .hintString("x_1");

        expectedOperation
                .hint(new BsonString("x_1"));

        Flux.from(publisher).blockFirst();
        assertEquals(ReadPreference.primary(), executor.getReadPreference());
        VoidReadOperationThenCursorReadOperation operation = (VoidReadOperationThenCursorReadOperation) executor.getReadOperation();
        assertOperationIsTheSameAs(expectedOperation, operation.getReadOperation());
    }

    @DisplayName("Should build the expected AggregateOperation for $out when both hint and hint string are set")
    @Test
    void shouldBuildTheExpectedOperationsForDollarOutWithHintPlusHintString() {
        String collectionName = "collectionName";
        List<BsonDocument> pipeline = asList(BsonDocument.parse("{'$match': 1}"),
                BsonDocument.parse(format("{'$out': '%s'}", collectionName)));
        MongoNamespace collectionNamespace = new MongoNamespace(NAMESPACE.getDatabaseName(), collectionName);

        TestOperationExecutor executor = createOperationExecutor(asList(getBatchCursor(), getBatchCursor(), getBatchCursor(), null));
        AggregatePublisher<Document> publisher =
                new AggregatePublisherImpl<>(null, createMongoOperationPublisher(executor), pipeline, AggregationLevel.COLLECTION);

        AggregateToCollectionOperation expectedOperation = new AggregateToCollectionOperation(NAMESPACE, pipeline,
                ReadConcern.DEFAULT,
                WriteConcern.ACKNOWLEDGED);

        publisher
                .hint(new Document("x", 1))
                .hintString("x_1");

        expectedOperation
                .hint(new BsonDocument("x", new BsonInt32(1)));

        Flux.from(publisher).blockFirst();
        assertEquals(ReadPreference.primary(), executor.getReadPreference());
        VoidReadOperationThenCursorReadOperation operation = (VoidReadOperationThenCursorReadOperation) executor.getReadOperation();
        assertOperationIsTheSameAs(expectedOperation, operation.getReadOperation());
    }

    @DisplayName("Should build the expected AggregateOperation for $out as document")
    @Test
    void shouldBuildTheExpectedOperationsForDollarOutAsDocument() {
        List<BsonDocument> pipeline = asList(BsonDocument.parse("{'$match': 1}"), BsonDocument.parse("{'$out': {s3: true}}"));

        TestOperationExecutor executor = createOperationExecutor(asList(null, null, null, null));
        AggregatePublisher<Document> publisher =
                new AggregatePublisherImpl<>(null, createMongoOperationPublisher(executor), pipeline, AggregationLevel.COLLECTION);

        // default input should be as expected
        assertThrows(IllegalStateException.class, () -> Flux.from(publisher).blockFirst());

        // Should handle toCollection
        Publisher<Void> toCollectionPublisher =
                new AggregatePublisherImpl<>(null, createMongoOperationPublisher(executor), pipeline, AggregationLevel.COLLECTION)
                        .toCollection();

        AggregateToCollectionOperation expectedOperation = new AggregateToCollectionOperation(NAMESPACE, pipeline, ReadConcern.DEFAULT,
                                                                                              WriteConcern.ACKNOWLEDGED);

        Flux.from(toCollectionPublisher).blockFirst();
        assertOperationIsTheSameAs(expectedOperation, executor.getReadOperation());

        // Should handle database level
        toCollectionPublisher =
                new AggregatePublisherImpl<>(null, createMongoOperationPublisher(executor), pipeline, AggregationLevel.DATABASE)
                        .toCollection();

        Flux.from(toCollectionPublisher).blockFirst();
        assertOperationIsTheSameAs(expectedOperation, executor.getReadOperation());

        // Should handle $out with namespace
        List<BsonDocument> pipelineWithNamespace = asList(BsonDocument.parse("{'$match': 1}"),
                                                          BsonDocument.parse("{'$out': {db: 'db1', coll: 'coll1'}}"));
        toCollectionPublisher = new AggregatePublisherImpl<>(null, createMongoOperationPublisher(executor), pipelineWithNamespace,
                                                             AggregationLevel.COLLECTION)
                .toCollection();

        expectedOperation = new AggregateToCollectionOperation(NAMESPACE, pipelineWithNamespace, ReadConcern.DEFAULT,
                                                               WriteConcern.ACKNOWLEDGED);

        Flux.from(toCollectionPublisher).blockFirst();
        assertOperationIsTheSameAs(expectedOperation, executor.getReadOperation());
    }

    @DisplayName("Should build the expected AggregateOperation for $merge document")
    @Test
    void shouldBuildTheExpectedOperationsForDollarMergeDocument() {
        String collectionName = "collectionName";
        List<BsonDocument> pipeline = asList(BsonDocument.parse("{'$match': 1}"),
                                             BsonDocument.parse(format("{'$merge': {into: '%s'}}", collectionName)));
        MongoNamespace collectionNamespace = new MongoNamespace(NAMESPACE.getDatabaseName(), collectionName);

        TestOperationExecutor executor = createOperationExecutor(asList(getBatchCursor(), getBatchCursor(), getBatchCursor(), null));
        AggregatePublisher<Document> publisher =
                new AggregatePublisherImpl<>(null, createMongoOperationPublisher(executor), pipeline, AggregationLevel.COLLECTION);

        AggregateToCollectionOperation expectedOperation = new AggregateToCollectionOperation(NAMESPACE, pipeline,
                                                                                              ReadConcern.DEFAULT,
                                                                                              WriteConcern.ACKNOWLEDGED);

        // default input should be as expected
        Flux.from(publisher).blockFirst();

        VoidReadOperationThenCursorReadOperation operation = (VoidReadOperationThenCursorReadOperation) executor.getReadOperation();
        assertEquals(ReadPreference.primary(), executor.getReadPreference());
        assertOperationIsTheSameAs(expectedOperation, operation.getReadOperation());

        // Should apply settings
        publisher
                .allowDiskUse(true)
                .batchSize(100) // Used in Find
                .bypassDocumentValidation(true)
                .collation(COLLATION)
                .comment(new BsonInt32(1))
                .hint(BsonDocument.parse("{a: 1}"))
                .maxAwaitTime(20, SECONDS) // Ignored on $out
                .maxTime(10, SECONDS);

        expectedOperation
                .allowDiskUse(true)
                .bypassDocumentValidation(true)
                .collation(COLLATION)
                .comment(new BsonInt32(1))
                .hint(BsonDocument.parse("{a: 1}"))
                .maxTime(10, SECONDS);

        Flux.from(publisher).blockFirst();
        assertEquals(ReadPreference.primary(), executor.getReadPreference());
        operation = (VoidReadOperationThenCursorReadOperation) executor.getReadOperation();
        assertOperationIsTheSameAs(expectedOperation, operation.getReadOperation());

        FindOperation<Document> expectedFindOperation =
                new FindOperation<>(collectionNamespace, getDefaultCodecRegistry().get(Document.class))
                        .batchSize(100)
                        .collation(COLLATION)
                        .filter(new BsonDocument())
                        .maxAwaitTime(0, SECONDS)
                        .maxTime(0, SECONDS)
                        .comment(new BsonInt32(1))
                        .retryReads(true);

        assertOperationIsTheSameAs(expectedFindOperation, operation.getCursorReadOperation());

        // Should handle database level aggregations
        publisher = new AggregatePublisherImpl<>(null, createMongoOperationPublisher(executor), pipeline, AggregationLevel.DATABASE);

        expectedOperation = new AggregateToCollectionOperation(NAMESPACE, pipeline, ReadConcern.DEFAULT, WriteConcern.ACKNOWLEDGED);

        Flux.from(publisher).blockFirst();
        operation = (VoidReadOperationThenCursorReadOperation) executor.getReadOperation();
        assertEquals(ReadPreference.primary(), executor.getReadPreference());
        assertOperationIsTheSameAs(expectedOperation, operation.getReadOperation());

        // Should handle toCollection
        publisher = new AggregatePublisherImpl<>(null, createMongoOperationPublisher(executor), pipeline, AggregationLevel.COLLECTION);

        expectedOperation = new AggregateToCollectionOperation(NAMESPACE, pipeline, ReadConcern.DEFAULT, WriteConcern.ACKNOWLEDGED);

        // default input should be as expected
        Flux.from(publisher.toCollection()).blockFirst();
        assertOperationIsTheSameAs(expectedOperation, executor.getReadOperation());
    }

    @DisplayName("Should build the expected AggregateOperation for $merge string")
    @Test
    void shouldBuildTheExpectedOperationsForDollarMergeString() {
        String collectionName = "collectionName";
        MongoNamespace collectionNamespace = new MongoNamespace(NAMESPACE.getDatabaseName(), collectionName);
        List<BsonDocument> pipeline = asList(BsonDocument.parse("{'$match': 1}"),
                BsonDocument.parse(format("{'$merge': '%s'}", collectionName)));

        TestOperationExecutor executor = createOperationExecutor(asList(getBatchCursor(), getBatchCursor(), getBatchCursor(), null));
        AggregatePublisher<Document> publisher =
                new AggregatePublisherImpl<>(null, createMongoOperationPublisher(executor), pipeline, AggregationLevel.COLLECTION);

        AggregateToCollectionOperation expectedOperation = new AggregateToCollectionOperation(NAMESPACE, pipeline,
                ReadConcern.DEFAULT,
                WriteConcern.ACKNOWLEDGED);

        // default input should be as expected
        Flux.from(publisher).blockFirst();

        VoidReadOperationThenCursorReadOperation operation = (VoidReadOperationThenCursorReadOperation) executor.getReadOperation();
        assertEquals(ReadPreference.primary(), executor.getReadPreference());
        assertOperationIsTheSameAs(expectedOperation, operation.getReadOperation());

        FindOperation<Document> expectedFindOperation =
                new FindOperation<>(collectionNamespace, getDefaultCodecRegistry().get(Document.class))
                .filter(new BsonDocument())
                .batchSize(Integer.MAX_VALUE)
                .retryReads(true);

        assertOperationIsTheSameAs(expectedFindOperation, operation.getCursorReadOperation());
    }

    @DisplayName("Should handle error scenarios")
    @Test
    void shouldHandleErrorScenarios() {
        List<BsonDocument> pipeline = singletonList(BsonDocument.parse("{'$match': 1}"));
        TestOperationExecutor executor = createOperationExecutor(asList(new MongoException("Failure"), null, null));

        // Operation fails
        Publisher<Document> publisher =
                new AggregatePublisherImpl<>(null, createMongoOperationPublisher(executor), pipeline, AggregationLevel.COLLECTION);
        assertThrows(MongoException.class, () -> Flux.from(publisher).blockFirst());

        // Missing Codec
        Publisher<Document> publisherMissingCodec =
                new AggregatePublisherImpl<>(null, createMongoOperationPublisher(executor)
                        .withCodecRegistry(BSON_CODEC_REGISTRY), pipeline, AggregationLevel.COLLECTION);
        assertThrows(CodecConfigurationException.class, () -> Flux.from(publisherMissingCodec).blockFirst());

        // Pipeline contains null
        Publisher<Document> publisherPipelineNull =
                new AggregatePublisherImpl<>(null, createMongoOperationPublisher(executor), singletonList(null),
                                             AggregationLevel.COLLECTION);
        assertThrows(IllegalArgumentException.class, () -> Flux.from(publisherPipelineNull).blockFirst());
    }


}
