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

package com.mongodb.client.gridfs;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.BsonDocument;
import org.bson.BsonMinKey;
import org.bson.BsonObjectId;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Asserts that the operations taking a file id wrap the user-supplied id in {@code $eq}, so that an id that is itself a document cannot
 * be interpreted as a query predicate and affect files other than the one identified by the id.
 */
final class GridFSBucketQueryFilterTest {

    @SuppressWarnings("unchecked")
    private final MongoCollection<GridFSFile> filesCollection = mock(MongoCollection.class);
    @SuppressWarnings("unchecked")
    private final MongoCollection<BsonDocument> chunksCollection = mock(MongoCollection.class);
    private final GridFSBucket bucket = new GridFSBucketImpl("fs", 255, filesCollection, chunksCollection);

    private static Stream<BsonValue> ids() {
        return Stream.of(
                new BsonObjectId(new ObjectId()),
                new BsonDocument("$gt", new BsonMinKey()),
                new BsonDocument("$ne", new BsonDocument("$exists", new BsonObjectId(new ObjectId()))));
    }

    @BeforeEach
    void setUp() {
        // Mockito answers 0L rather than null for the Long-returning getTimeout, which would make the bucket apply a 0ms timeout.
        when(filesCollection.getTimeout(any(TimeUnit.class))).thenReturn(null);
        when(chunksCollection.getTimeout(any(TimeUnit.class))).thenReturn(null);

        when(filesCollection.deleteOne(any(Bson.class))).thenReturn(DeleteResult.acknowledged(1));
        when(filesCollection.deleteOne(any(ClientSession.class), any(Bson.class))).thenReturn(DeleteResult.acknowledged(1));
    }

    @ParameterizedTest
    @MethodSource("ids")
    void deleteUsesEqualityFilters(final BsonValue id) {
        bucket.delete(id);

        ArgumentCaptor<Bson> filesFilter = ArgumentCaptor.forClass(Bson.class);
        ArgumentCaptor<Bson> chunksFilter = ArgumentCaptor.forClass(Bson.class);
        verify(filesCollection).deleteOne(filesFilter.capture());
        verify(chunksCollection).deleteMany(chunksFilter.capture());

        //verify that id is wrapped in $eq
        assertEquals(expected("_id", id), toBsonDocument(filesFilter.getValue()));
        assertEquals(expected("files_id", id), toBsonDocument(chunksFilter.getValue()));
    }

    @ParameterizedTest
    @MethodSource("ids")
    void renameUsesEqualityFilter(final BsonValue id) {
        when(filesCollection.updateOne(any(Bson.class), any(Bson.class))).thenReturn(UpdateResult.acknowledged(1, 1L, null));

        bucket.rename(id, "newFilename");

        ArgumentCaptor<Bson> filesFilter = ArgumentCaptor.forClass(Bson.class);
        verify(filesCollection).updateOne(filesFilter.capture(), any(Bson.class));
        assertEquals(expected("_id", id), toBsonDocument(filesFilter.getValue()));
    }

    @ParameterizedTest
    @MethodSource("ids")
    @SuppressWarnings("unchecked")
    void openDownloadStreamUsesEqualityFilter(final BsonValue id) {
        FindIterable<GridFSFile> findIterable = mock(FindIterable.class);
        when(filesCollection.find()).thenReturn(findIterable);
        when(findIterable.filter(any(Bson.class))).thenReturn(findIterable);
        when(findIterable.first()).thenReturn(new GridFSFile(id, "filename", 10L, 255, new Date(), new Document()));

        bucket.openDownloadStream(id);

        ArgumentCaptor<Bson> filesFilter = ArgumentCaptor.forClass(Bson.class);
        verify(findIterable).filter(filesFilter.capture());
        assertEquals(expected("_id", id), toBsonDocument(filesFilter.getValue()));
    }

    @ParameterizedTest
    @MethodSource("ids")
    void abortUsesEqualityFilter(final BsonValue id) {
        GridFSUploadStream uploadStream = new GridFSUploadStreamImpl(null, filesCollection, chunksCollection, id, "filename", 255,
                null, null);

        uploadStream.abort();

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
