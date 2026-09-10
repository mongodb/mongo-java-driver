/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.reactivestreams.client.internal.gridfs;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoGridFSException;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.reactivestreams.client.FindPublisher;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import com.mongodb.reactivestreams.client.gridfs.GridFSBucket;
import org.bson.BsonDocument;
import org.bson.BsonMinKey;
import org.bson.BsonObjectId;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Asserts that the GridFS publishers taking a file id wrap the user-supplied id in {@code $eq}, so that an id that is itself a document
 * cannot be interpreted as a query predicate and affect files other than the one identified by the id.
 */
final class GridFSQueryFilterTest {

    private static Stream<BsonValue> ids() {
        return Stream.of(
                new BsonObjectId(new ObjectId()),
                new BsonDocument("$gt", new BsonMinKey()),
                new BsonDocument("$ne", new BsonDocument("$exists", new BsonObjectId(new ObjectId()))));
    }

    @ParameterizedTest
    @MethodSource("ids")
    @SuppressWarnings("unchecked")
    void deleteUsesEqualityFilters(final BsonValue id) {
        MongoCollection<GridFSFile> filesCollection = mock(MongoCollection.class);
        MongoCollection<Document> chunksCollection = mock(MongoCollection.class);
        // Mockito answers 0L rather than null for the Long-returning getTimeout, which would make the publisher apply a 0ms timeout.
        when(filesCollection.getTimeout(any(TimeUnit.class))).thenReturn(null);
        when(chunksCollection.getTimeout(any(TimeUnit.class))).thenReturn(null);
        when(filesCollection.deleteOne(any(Bson.class))).thenReturn(Mono.just(DeleteResult.acknowledged(1)));
        when(chunksCollection.deleteMany(any(Bson.class))).thenReturn(Mono.just(DeleteResult.acknowledged(1)));

        Mono.from(GridFSPublisherCreator.createDeletePublisher(filesCollection, chunksCollection, null, id)).block();

        ArgumentCaptor<Bson> filesFilter = ArgumentCaptor.forClass(Bson.class);
        ArgumentCaptor<Bson> chunksFilter = ArgumentCaptor.forClass(Bson.class);
        verify(filesCollection).deleteOne(filesFilter.capture());
        verify(chunksCollection).deleteMany(chunksFilter.capture());

        assertEquals(expected("_id", id), toBsonDocument(filesFilter.getValue()));
        assertEquals(expected("files_id", id), toBsonDocument(chunksFilter.getValue()));
    }

    @ParameterizedTest
    @MethodSource("ids")
    @SuppressWarnings("unchecked")
    void renameUsesEqualityFilter(final BsonValue id) {
        MongoCollection<GridFSFile> filesCollection = mock(MongoCollection.class);
        when(filesCollection.getTimeout(any(TimeUnit.class))).thenReturn(null);
        when(filesCollection.updateOne(any(Bson.class), any(Bson.class)))
                .thenReturn(Mono.just(UpdateResult.acknowledged(1, 1L, null)));

        Mono.from(GridFSPublisherCreator.createRenamePublisher(filesCollection, null, id, "newFilename")).block();

        ArgumentCaptor<Bson> filesFilter = ArgumentCaptor.forClass(Bson.class);
        verify(filesCollection).updateOne(filesFilter.capture(), any(Bson.class));
        assertEquals(expected("_id", id), toBsonDocument(filesFilter.getValue()));
    }

    @ParameterizedTest
    @MethodSource("ids")
    @SuppressWarnings("unchecked")
    void downloadToPublisherUsesEqualityFilter(final BsonValue id) {
        MongoDatabase database = mock(MongoDatabase.class);
        MongoCollection<GridFSFile> rawFilesCollection = mock(MongoCollection.class);
        MongoCollection<GridFSFile> filesCollection = mock(MongoCollection.class);
        MongoCollection<Document> rawChunksCollection = mock(MongoCollection.class);
        MongoCollection<Document> chunksCollection = mock(MongoCollection.class);
        FindPublisher<GridFSFile> findPublisher = mock(FindPublisher.class);

        //setup: reactive driver doesn't have a ctor. that takes collection in args
        // so we first have to mock the database and it's behavior
        when(database.getCodecRegistry()).thenReturn(MongoClientSettings.getDefaultCodecRegistry());
        when(database.getCollection("fs.files", GridFSFile.class)).thenReturn(rawFilesCollection);
        when(rawFilesCollection.withCodecRegistry(any())).thenReturn(filesCollection);
        when(database.getCollection("fs.chunks")).thenReturn(rawChunksCollection);
        when(rawChunksCollection.withCodecRegistry(any())).thenReturn(chunksCollection);
        when(filesCollection.getTimeout(any(TimeUnit.class))).thenReturn(null);
        when(chunksCollection.getTimeout(any(TimeUnit.class))).thenReturn(null);
        when(filesCollection.find()).thenReturn(findPublisher);
        when(findPublisher.filter(any(Bson.class))).thenReturn(findPublisher);

        doAnswer(invocation -> {
            Mono.<GridFSFile>empty().subscribe(invocation.getArgument(0, Subscriber.class));
            return null;
        }).when(findPublisher).subscribe(any());

        GridFSBucket bucket = new GridFSBucketImpl(database);

        assertThrows(MongoGridFSException.class, () -> Mono.from(bucket.downloadToPublisher(id)).block());

        ArgumentCaptor<Bson> filesFilter = ArgumentCaptor.forClass(Bson.class);
        verify(findPublisher).filter(filesFilter.capture());
        assertEquals(expected("_id", id), toBsonDocument(filesFilter.getValue()));
    }

    /**
     * The upload publisher deletes the chunks written so far when the upload fails, so an operator-shaped id would delete chunks
     * belonging to other files. This path is driven here by an erroring source publisher.
     */
    @ParameterizedTest
    @MethodSource("ids")
    @SuppressWarnings("unchecked")
    void uploadCancellationUsesEqualityFilter(final BsonValue id) {
        MongoCollection<GridFSFile> filesCollection = mock(MongoCollection.class);
        MongoCollection<Document> filesAsDocuments = mock(MongoCollection.class);
        MongoCollection<Document> chunksCollection = mock(MongoCollection.class);
        FindPublisher<Document> findPublisher = mock(FindPublisher.class);

        when(filesCollection.getTimeout(any(TimeUnit.class))).thenReturn(null);
        when(filesCollection.withDocumentClass(Document.class)).thenReturn(filesAsDocuments);
        when(filesAsDocuments.withReadPreference(any())).thenReturn(filesAsDocuments);
        when(filesAsDocuments.getTimeout(any(TimeUnit.class))).thenReturn(null);
        when(filesAsDocuments.find()).thenReturn(findPublisher);
        when(findPublisher.projection(any())).thenReturn(findPublisher);
        // A non-empty files collection, so the index checks are skipped.
        when(findPublisher.first()).thenReturn(Mono.just(new Document()));
        when(chunksCollection.getTimeout(any(TimeUnit.class))).thenReturn(null);
        when(chunksCollection.deleteMany(any(Bson.class))).thenReturn(Mono.just(DeleteResult.acknowledged(0)));

        GridFSUploadPublisherImpl uploadPublisher = new GridFSUploadPublisherImpl(null, filesCollection, chunksCollection, id,
                "filename", 255, null, Flux.error(new RuntimeException("source failed")));

        assertThrows(RuntimeException.class, () -> Mono.from(uploadPublisher).block());

        ArgumentCaptor<Bson> chunksFilter = ArgumentCaptor.forClass(Bson.class);
        verify(chunksCollection).deleteMany(chunksFilter.capture());
        assertEquals(expected("files_id", id), toBsonDocument(chunksFilter.getValue()));
    }

    private static BsonDocument expected(final String fieldName, final BsonValue id) {
        return new BsonDocument(fieldName, new BsonDocument("$eq", id));
    }

    private static BsonDocument toBsonDocument(final Bson filter) {
        return filter.toBsonDocument(BsonDocument.class, MongoClientSettings.getDefaultCodecRegistry());
    }
}
