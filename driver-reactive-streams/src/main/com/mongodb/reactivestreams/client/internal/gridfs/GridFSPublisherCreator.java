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

import com.mongodb.MongoGridFSException;
import com.mongodb.client.cursor.TimeoutMode;
import com.mongodb.client.gridfs.model.GridFSDownloadOptions;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.time.Timeout;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.FindPublisher;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.gridfs.GridFSFindPublisher;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.util.function.Function;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.reactivestreams.client.internal.gridfs.CollectionTimeoutHelper.withNullableTimeout;
import static com.mongodb.reactivestreams.client.internal.gridfs.CollectionTimeoutHelper.withNullableTimeoutMono;
import static com.mongodb.reactivestreams.client.internal.gridfs.CollectionTimeoutHelper.withNullableTimeoutMonoDeferred;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class GridFSPublisherCreator {

    private GridFSPublisherCreator() {
    }

    public static GridFSUploadPublisherImpl createGridFSUploadPublisher(
            final int chunkSizeBytes, final MongoCollection<GridFSFile> filesCollection, final MongoCollection<Document> chunksCollection,
            @Nullable final ClientSession clientSession, final BsonValue id, final String filename, final GridFSUploadOptions options,
            final Publisher<ByteBuffer> source) {
        notNull("filesCollection", filesCollection);
        notNull("chunksCollection", chunksCollection);
        notNull("id", id);
        notNull("filename", filename);
        notNull("options", options);
        Integer chunkSize = options.getChunkSizeBytes();
        if (chunkSize == null) {
            chunkSize = chunkSizeBytes;
        }
        return new GridFSUploadPublisherImpl(clientSession, filesCollection, chunksCollection, id, filename, chunkSize,
                                             options.getMetadata(), source);
    }

    public static GridFSDownloadPublisherImpl createGridFSDownloadPublisher(
            final MongoCollection<Document> chunksCollection,
            @Nullable final ClientSession clientSession,
            final Function<Timeout, GridFSFindPublisher> publisher) {
        notNull("chunksCollection", chunksCollection);
        notNull("gridFSFileMono", publisher);
        return new GridFSDownloadPublisherImpl(clientSession, publisher, chunksCollection);
    }

    public static GridFSFindPublisher createGridFSFindPublisher(
            final MongoCollection<GridFSFile> filesCollection,
            @Nullable final ClientSession clientSession,
            @Nullable final Bson filter) {
        notNull("filesCollection", filesCollection);
        return new GridFSFindPublisherImpl(createFindPublisher(filesCollection, clientSession, filter));
    }

    public static GridFSFindPublisher createGridFSFindPublisher(
            final MongoCollection<GridFSFile> filesCollection,
            @Nullable final ClientSession clientSession,
            @Nullable final Bson filter,
            @Nullable final Timeout operationTimeout) {
        notNull("filesCollection", filesCollection);
        return new GridFSFindPublisherImpl(createFindPublisher(filesCollection, clientSession, filter, operationTimeout));
    }

    public static GridFSFindPublisher createGridFSFindPublisher(
            final MongoCollection<GridFSFile> filesCollection,
            @Nullable final ClientSession clientSession,
            final String filename,
            final GridFSDownloadOptions options,
            @Nullable final Timeout operationTimeout) {
        notNull("filesCollection", filesCollection);
        notNull("filename", filename);
        notNull("options", options);

        int revision = options.getRevision();
        int skip;
        int sort;
        if (revision >= 0) {
            skip = revision;
            sort = 1;
        } else {
            skip = (-revision) - 1;
            sort = -1;
        }

        return createGridFSFindPublisher(filesCollection, clientSession, new Document("filename", filename), operationTimeout).skip(skip)
                .sort(new Document("uploadDate", sort));
    }

    public static FindPublisher<GridFSFile> createFindPublisher(
            final MongoCollection<GridFSFile> filesCollection,
            @Nullable final ClientSession clientSession,
            @Nullable final Bson filter,
            @Nullable final Timeout operationTimeout) {
        notNull("filesCollection", filesCollection);
        FindPublisher<GridFSFile> publisher;
        if (clientSession == null) {
            publisher = withNullableTimeout(filesCollection, operationTimeout).find();
        } else {
            publisher = withNullableTimeout(filesCollection, operationTimeout).find(clientSession);
        }

        if (filter != null) {
            publisher = publisher.filter(filter);
        }
        if (operationTimeout != null) {
            publisher.timeoutMode(TimeoutMode.CURSOR_LIFETIME);
        }
        return publisher;
    }

    public static FindPublisher<GridFSFile> createFindPublisher(
            final MongoCollection<GridFSFile> filesCollection,
            @Nullable final ClientSession clientSession,
            @Nullable final Bson filter) {
        notNull("filesCollection", filesCollection);
        FindPublisher<GridFSFile> publisher;
        if (clientSession == null) {
            publisher = filesCollection.find();
        } else {
            publisher = filesCollection.find(clientSession);
        }
        if (filter != null) {
            publisher = publisher.filter(filter);
        }
        if (filesCollection.getTimeout(MILLISECONDS) != null) {
            publisher.timeoutMode(TimeoutMode.CURSOR_LIFETIME);
        }
        return publisher;
    }

    public static Publisher<Void> createDeletePublisher(final MongoCollection<GridFSFile> filesCollection,
                                                        final MongoCollection<Document> chunksCollection,
                                                        @Nullable final ClientSession clientSession,
                                                        final BsonValue id) {
        notNull("filesCollection", filesCollection);
        notNull("chunksCollection", chunksCollection);
        notNull("id", id);
        BsonDocument filter = new BsonDocument("_id", id);

        return Mono.defer(()-> {
            Timeout operationTimeout = startTimeout(filesCollection.getTimeout(MILLISECONDS));
            return withNullableTimeoutMono(filesCollection, operationTimeout)
                    .flatMap(wrappedCollection -> {
                    if (clientSession == null) {
                        return Mono.from(wrappedCollection.deleteOne(filter));
                    } else {
                        return Mono.from(wrappedCollection.deleteOne(clientSession, filter));
                    }
                }).flatMap(deleteResult -> {
                    if (deleteResult.wasAcknowledged() && deleteResult.getDeletedCount() == 0) {
                        return Mono.error(new MongoGridFSException(format("No file found with the ObjectId: %s", id)));
                    }
                    return withNullableTimeoutMono(chunksCollection, operationTimeout);
                }).flatMap(wrappedCollection -> {
                    if (clientSession == null) {
                        return Mono.from(wrappedCollection.deleteMany(new BsonDocument("files_id", id)));
                    } else {
                        return Mono.from(wrappedCollection.deleteMany(clientSession, new BsonDocument("files_id", id)));
                    }
                }).then();
            });
    }

    public static Publisher<Void> createRenamePublisher(final MongoCollection<GridFSFile> filesCollection,
                                                        @Nullable final ClientSession clientSession,
                                                        final BsonValue id,
                                                        final String newFilename) {
        notNull("filesCollection", filesCollection);
        notNull("id", id);
        notNull("newFilename", newFilename);
        BsonDocument filter = new BsonDocument("_id", id);
        BsonDocument update = new BsonDocument("$set",
                                               new BsonDocument("filename", new BsonString(newFilename)));
        Publisher<UpdateResult> publisher;
        if (clientSession == null) {
            publisher = filesCollection.updateOne(filter, update);
        } else {
            publisher = filesCollection.updateOne(clientSession, filter, update);
        }

        return Mono.from(publisher).flatMap(updateResult -> {
            if (updateResult.wasAcknowledged() && updateResult.getModifiedCount() == 0) {
                throw new MongoGridFSException(format("No file found with the ObjectId: %s", id));
            }
            return Mono.empty();
        });
    }

    public static Publisher<Void> createDropPublisher(final MongoCollection<GridFSFile> filesCollection,
                                                      final MongoCollection<Document> chunksCollection,
                                                      @Nullable final ClientSession clientSession) {

        return Mono.defer(() -> {
            Timeout operationTimeout = startTimeout(filesCollection.getTimeout(MILLISECONDS));
            return withNullableTimeoutMono(filesCollection, operationTimeout)
                    .flatMap(wrappedCollection -> {
                        if (clientSession == null) {
                            return Mono.from(wrappedCollection.drop());
                        } else {
                            return Mono.from(wrappedCollection.drop(clientSession));
                        }
                    }).then(withNullableTimeoutMonoDeferred(chunksCollection, operationTimeout))
                    .flatMap(wrappedCollection -> {
                        if (clientSession == null) {
                            return Mono.from(wrappedCollection.drop());
                        } else {
                            return Mono.from(wrappedCollection.drop(clientSession));
                        }

                    });
        });
    }

    @Nullable
    private static Timeout startTimeout(@Nullable final Long timeoutMs) {
        return timeoutMs == null ? null : TimeoutContext.calculateTimeout(timeoutMs);
    }
}
