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

package com.mongodb.reactivestreams.client.internal;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.gridfs.model.GridFSDownloadOptions;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import com.mongodb.internal.async.client.gridfs.AsyncGridFSBucket;
import com.mongodb.internal.async.client.gridfs.AsyncGridFSDownloadStream;
import com.mongodb.internal.async.client.gridfs.AsyncGridFSUploadStream;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.gridfs.GridFSBucket;
import com.mongodb.reactivestreams.client.gridfs.GridFSDownloadPublisher;
import com.mongodb.reactivestreams.client.gridfs.GridFSFindPublisher;
import com.mongodb.reactivestreams.client.gridfs.GridFSUploadPublisher;
import org.bson.BsonObjectId;
import org.bson.BsonValue;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.reactivestreams.Publisher;

import java.nio.ByteBuffer;

import static com.mongodb.assertions.Assertions.notNull;


/**
 * The internal GridFSBucket implementation.
 *
 * <p>This should not be considered a part of the public API.</p>
 */
public final class GridFSBucketImpl implements GridFSBucket {
    private final AsyncGridFSBucket wrapped;

    /**
     * The GridFSBucket constructor
     *
     * <p>This should not be considered a part of the public API.</p>
     *
     * @param wrapped the GridFSBucket
     */
    public GridFSBucketImpl(final AsyncGridFSBucket wrapped) {
        this.wrapped = notNull("GridFSBucket", wrapped);
    }

    @Override
    public String getBucketName() {
        return wrapped.getBucketName();
    }

    @Override
    public int getChunkSizeBytes() {
        return wrapped.getChunkSizeBytes();
    }

    @Override
    public ReadPreference getReadPreference() {
        return wrapped.getReadPreference();
    }

    @Override
    public WriteConcern getWriteConcern() {
        return wrapped.getWriteConcern();
    }

    @Override
    public ReadConcern getReadConcern() {
        return wrapped.getReadConcern();
    }

    @Override
    public GridFSBucket withChunkSizeBytes(final int chunkSizeBytes) {
        return new GridFSBucketImpl(wrapped.withChunkSizeBytes(chunkSizeBytes));
    }

    @Override
    public GridFSBucket withReadPreference(final ReadPreference readPreference) {
        return new GridFSBucketImpl(wrapped.withReadPreference(readPreference));
    }

    @Override
    public GridFSBucket withWriteConcern(final WriteConcern writeConcern) {
        return new GridFSBucketImpl(wrapped.withWriteConcern(writeConcern));
    }

    @Override
    public GridFSBucket withReadConcern(final ReadConcern readConcern) {
        return new GridFSBucketImpl(wrapped.withReadConcern(readConcern));
    }

    @Override
    public GridFSUploadPublisher<ObjectId> uploadFromPublisher(final String filename, final Publisher<ByteBuffer> source) {
        return uploadFromPublisher(filename, source, new GridFSUploadOptions());
    }

    @Override
    public GridFSUploadPublisher<ObjectId> uploadFromPublisher(final String filename, final Publisher<ByteBuffer> source,
                                                               final GridFSUploadOptions options) {
        return executeUploadFromPublisher(wrapped.openUploadStream(new BsonObjectId(), filename, options), source).withObjectId();
    }

    @Override
    public GridFSUploadPublisher<Void> uploadFromPublisher(final BsonValue id, final String filename,
                                                              final Publisher<ByteBuffer> source) {
        return uploadFromPublisher(id, filename, source, new GridFSUploadOptions());
    }

    @Override
    public GridFSUploadPublisher<Void> uploadFromPublisher(final BsonValue id, final String filename,
                                                              final Publisher<ByteBuffer> source, final GridFSUploadOptions options) {
        return executeUploadFromPublisher(wrapped.openUploadStream(id, filename, options), source);
    }

    @Override
    public GridFSUploadPublisher<ObjectId> uploadFromPublisher(final ClientSession clientSession, final String filename,
                                                               final Publisher<ByteBuffer> source) {
        return uploadFromPublisher(clientSession, filename, source, new GridFSUploadOptions());
    }

    @Override
    public GridFSUploadPublisher<ObjectId> uploadFromPublisher(final ClientSession clientSession, final String filename,
                                                               final Publisher<ByteBuffer> source, final GridFSUploadOptions options) {
        return executeUploadFromPublisher(wrapped.openUploadStream(clientSession.getWrapped(), new BsonObjectId(), filename, options),
                source).withObjectId();
    }

    @Override
    public GridFSUploadPublisher<Void> uploadFromPublisher(final ClientSession clientSession, final BsonValue id, final String filename,
                                                           final Publisher<ByteBuffer> source) {
        return uploadFromPublisher(clientSession, id, filename, source, new GridFSUploadOptions());
    }

    @Override
    public GridFSUploadPublisher<Void> uploadFromPublisher(final ClientSession clientSession, final BsonValue id, final String filename,
                                                              final Publisher<ByteBuffer> source, final GridFSUploadOptions options) {
        return executeUploadFromPublisher(wrapped.openUploadStream(clientSession.getWrapped(), id, filename, options), source);
    }

    @Override
    public GridFSDownloadPublisher downloadToPublisher(final ObjectId id) {
        return executeDownloadToPublisher(wrapped.openDownloadStream(id));
    }

    @Override
    public GridFSDownloadPublisher downloadToPublisher(final BsonValue id) {
        return executeDownloadToPublisher(wrapped.openDownloadStream(id));
    }

    @Override
    public GridFSDownloadPublisher downloadToPublisher(final String filename) {
        return executeDownloadToPublisher(wrapped.openDownloadStream(filename));
    }

    @Override
    public GridFSDownloadPublisher downloadToPublisher(final String filename, final GridFSDownloadOptions options) {
        return executeDownloadToPublisher(wrapped.openDownloadStream(filename, options));
    }

    @Override
    public GridFSDownloadPublisher downloadToPublisher(final ClientSession clientSession, final ObjectId id) {
        return executeDownloadToPublisher(wrapped.openDownloadStream(clientSession.getWrapped(), id));
    }

    @Override
    public GridFSDownloadPublisher downloadToPublisher(final ClientSession clientSession, final BsonValue id) {
        return executeDownloadToPublisher(wrapped.openDownloadStream(clientSession.getWrapped(), id));
    }

    @Override
    public GridFSDownloadPublisher downloadToPublisher(final ClientSession clientSession, final String filename) {
        return executeDownloadToPublisher(wrapped.openDownloadStream(clientSession.getWrapped(), filename));
    }

    @Override
    public GridFSDownloadPublisher downloadToPublisher(final ClientSession clientSession, final String filename,
                                                       final GridFSDownloadOptions options) {
        return executeDownloadToPublisher(wrapped.openDownloadStream(clientSession.getWrapped(), filename, options));
    }

    private GridFSDownloadPublisher
    executeDownloadToPublisher(final AsyncGridFSDownloadStream gridFSDownloadStream) {
        return new GridFSDownloadPublisherImpl(gridFSDownloadStream);
    }

    private GridFSUploadPublisherImpl
    executeUploadFromPublisher(final AsyncGridFSUploadStream gridFSUploadStream, final Publisher<ByteBuffer> source) {
        return new GridFSUploadPublisherImpl(gridFSUploadStream, source);
    }

    @Override
    public GridFSFindPublisher find() {
        return new GridFSFindPublisherImpl(wrapped.find());
    }

    @Override
    public GridFSFindPublisher find(final Bson filter) {
        return new GridFSFindPublisherImpl(wrapped.find(filter));
    }

    @Override
    public GridFSFindPublisher find(final ClientSession clientSession) {
        return new GridFSFindPublisherImpl(wrapped.find(clientSession.getWrapped()));
    }

    @Override
    public GridFSFindPublisher find(final ClientSession clientSession, final Bson filter) {
        return new GridFSFindPublisherImpl(wrapped.find(clientSession.getWrapped(), filter));
    }

    @Override
    public Publisher<Void> delete(final ObjectId id) {
        return Publishers.publish(callback -> wrapped.delete(id, callback));
    }

    @Override
    public Publisher<Void> delete(final BsonValue id) {
        return Publishers.publish(callback -> wrapped.delete(id, callback));
    }

    @Override
    public Publisher<Void> delete(final ClientSession clientSession, final ObjectId id) {
        return Publishers.publish(callback -> wrapped.delete(clientSession.getWrapped(), id, callback));
    }

    @Override
    public Publisher<Void> delete(final ClientSession clientSession, final BsonValue id) {
        return Publishers.publish(callback -> wrapped.delete(clientSession.getWrapped(), id, callback));
    }

    @Override
    public Publisher<Void> rename(final ObjectId id, final String newFilename) {
        return Publishers.publish(callback -> wrapped.rename(id, newFilename, callback));
    }

    @Override
    public Publisher<Void> rename(final BsonValue id, final String newFilename) {
        return Publishers.publish(callback -> wrapped.rename(id, newFilename, callback));
    }

    @Override
    public Publisher<Void> rename(final ClientSession clientSession, final ObjectId id, final String newFilename) {
        return Publishers.publish(callback ->
                wrapped.rename(clientSession.getWrapped(), id, newFilename, callback));
    }

    @Override
    public Publisher<Void> rename(final ClientSession clientSession, final BsonValue id, final String newFilename) {
        return Publishers.publish(callback ->
                wrapped.rename(clientSession.getWrapped(), id, newFilename, callback));
    }

    @Override
    public Publisher<Void> drop() {
        return Publishers.publish(wrapped::drop);
    }

    @Override
    public Publisher<Void> drop(final ClientSession clientSession) {
        return Publishers.publish(callback -> wrapped.drop(clientSession.getWrapped(), callback));
    }

}
