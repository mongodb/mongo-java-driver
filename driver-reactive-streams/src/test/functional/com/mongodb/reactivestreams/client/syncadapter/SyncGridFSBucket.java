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

package com.mongodb.reactivestreams.client.syncadapter;

import com.mongodb.MongoGridFSException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSDownloadStream;
import com.mongodb.client.gridfs.GridFSFindIterable;
import com.mongodb.client.gridfs.GridFSUploadStream;
import com.mongodb.client.gridfs.model.GridFSDownloadOptions;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import com.mongodb.reactivestreams.client.gridfs.GridFSDownloadPublisher;
import com.mongodb.reactivestreams.client.gridfs.GridFSUploadPublisher;
import org.bson.BsonObjectId;
import org.bson.BsonValue;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static com.mongodb.ClusterFixture.TIMEOUT_DURATION;
import static com.mongodb.reactivestreams.client.syncadapter.ContextHelper.CONTEXT;
import static java.util.Objects.requireNonNull;

public class SyncGridFSBucket implements GridFSBucket {
    private final com.mongodb.reactivestreams.client.gridfs.GridFSBucket wrapped;

    public SyncGridFSBucket(final com.mongodb.reactivestreams.client.gridfs.GridFSBucket wrapped) {
        this.wrapped = wrapped;
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
    public WriteConcern getWriteConcern() {
        return wrapped.getWriteConcern();
    }

    @Override
    public ReadPreference getReadPreference() {
        return wrapped.getReadPreference();
    }

    @Override
    public ReadConcern getReadConcern() {
        return wrapped.getReadConcern();
    }

    @Override
    public GridFSBucket withChunkSizeBytes(final int chunkSizeBytes) {
        return new SyncGridFSBucket(wrapped.withChunkSizeBytes(chunkSizeBytes));
    }

    @Override
    public GridFSBucket withReadPreference(final ReadPreference readPreference) {
        return new SyncGridFSBucket(wrapped.withReadPreference(readPreference));
    }

    @Override
    public GridFSBucket withWriteConcern(final WriteConcern writeConcern) {
        return new SyncGridFSBucket(wrapped.withWriteConcern(writeConcern));
    }

    @Override
    public GridFSBucket withReadConcern(final ReadConcern readConcern) {
        return new SyncGridFSBucket(wrapped.withReadConcern(readConcern));
    }

    @Override
    public GridFSUploadStream openUploadStream(final String filename) {
        return openUploadStream(filename, new GridFSUploadOptions());
    }

    @Override
    public GridFSUploadStream openUploadStream(final String filename, final GridFSUploadOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public GridFSUploadStream openUploadStream(final BsonValue id, final String filename) {
        return openUploadStream(id, filename, new GridFSUploadOptions());
    }

    @Override
    public GridFSUploadStream openUploadStream(final BsonValue id, final String filename, final GridFSUploadOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public GridFSUploadStream openUploadStream(final ClientSession clientSession, final String filename) {
        return openUploadStream(clientSession, filename, new GridFSUploadOptions());
    }

    @Override
    public GridFSUploadStream openUploadStream(final ClientSession clientSession, final String filename,
                                               final GridFSUploadOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public GridFSUploadStream openUploadStream(final ClientSession clientSession, final BsonValue id, final String filename) {
        return openUploadStream(clientSession, id, filename, new GridFSUploadOptions());
    }

    @Override
    public GridFSUploadStream openUploadStream(final ClientSession clientSession, final ObjectId id, final String filename) {
        return openUploadStream(clientSession, new BsonObjectId(id), filename, new GridFSUploadOptions());
    }

    @Override
    public GridFSUploadStream openUploadStream(final ClientSession clientSession, final BsonValue id, final String filename,
                                               final GridFSUploadOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ObjectId uploadFromStream(final String filename, final InputStream source) {
        return uploadFromStream(filename, source, new GridFSUploadOptions());
    }

    @Override
    public ObjectId uploadFromStream(final String filename, final InputStream source, final GridFSUploadOptions options) {
        Flux<ByteBuffer> sourceToPublisher = inputStreamToFlux(source, options);
        GridFSUploadPublisher<ObjectId> uploadPublisher = wrapped.uploadFromPublisher(filename, sourceToPublisher, options);
        return requireNonNull(Mono.from(uploadPublisher).contextWrite(CONTEXT).block(TIMEOUT_DURATION));
    }

    @Override
    public void uploadFromStream(final BsonValue id, final String filename, final InputStream source) {
        uploadFromStream(id, filename, source, new GridFSUploadOptions());
    }

    @Override
    public void uploadFromStream(final BsonValue id, final String filename, final InputStream source, final GridFSUploadOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ObjectId uploadFromStream(final ClientSession clientSession, final String filename, final InputStream source) {
        return uploadFromStream(clientSession, filename, source, new GridFSUploadOptions());
    }

    @Override
    public ObjectId uploadFromStream(final ClientSession clientSession, final String filename, final InputStream source,
                                     final GridFSUploadOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void uploadFromStream(final ClientSession clientSession, final BsonValue id, final String filename, final InputStream source) {
        uploadFromStream(clientSession, id, filename, source, new GridFSUploadOptions());
    }

    @Override
    public void uploadFromStream(final ClientSession clientSession, final BsonValue id, final String filename, final InputStream source,
                                 final GridFSUploadOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public GridFSDownloadStream openDownloadStream(final ObjectId id) {
        return openDownloadStream(new BsonObjectId(id));
    }

    @Override
    public GridFSDownloadStream openDownloadStream(final BsonValue id) {
        throw new UnsupportedOperationException();
    }

    @Override
    public GridFSDownloadStream openDownloadStream(final String filename) {
        return openDownloadStream(filename, new GridFSDownloadOptions());
    }

    @Override
    public GridFSDownloadStream openDownloadStream(final String filename, final GridFSDownloadOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public GridFSDownloadStream openDownloadStream(final ClientSession clientSession, final ObjectId id) {
        return openDownloadStream(clientSession, new BsonObjectId(id));
    }

    @Override
    public GridFSDownloadStream openDownloadStream(final ClientSession clientSession, final BsonValue id) {
        throw new UnsupportedOperationException();
    }

    @Override
    public GridFSDownloadStream openDownloadStream(final ClientSession clientSession, final String filename) {
        return openDownloadStream(clientSession, filename, new GridFSDownloadOptions());
    }

    @Override
    public GridFSDownloadStream openDownloadStream(final ClientSession clientSession, final String filename,
                                                   final GridFSDownloadOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void downloadToStream(final ObjectId id, final OutputStream destination) {
        downloadToStream(new BsonObjectId(id), destination);
    }

    @Override
    public void downloadToStream(final BsonValue id, final OutputStream destination) {
        toOutputStream(wrapped.downloadToPublisher(id), destination);
    }

    @Override
    public void downloadToStream(final String filename, final OutputStream destination) {
        downloadToStream(filename, destination, new GridFSDownloadOptions());
    }

    @Override
    public void downloadToStream(final String filename, final OutputStream destination, final GridFSDownloadOptions options) {
        toOutputStream(wrapped.downloadToPublisher(filename, options), destination);
    }

    @Override
    public void downloadToStream(final ClientSession clientSession, final ObjectId id, final OutputStream destination) {
        downloadToStream(clientSession, new BsonObjectId(id), destination);
    }

    @Override
    public void downloadToStream(final ClientSession clientSession, final BsonValue id, final OutputStream destination) {
        toOutputStream(wrapped.downloadToPublisher(unwrap(clientSession), id), destination);
    }

    @Override
    public void downloadToStream(final ClientSession clientSession, final String filename, final OutputStream destination) {
        downloadToStream(clientSession, filename, destination, new GridFSDownloadOptions());
    }

    @Override
    public void downloadToStream(final ClientSession clientSession, final String filename, final OutputStream destination,
                                 final GridFSDownloadOptions options) {
        toOutputStream(wrapped.downloadToPublisher(unwrap(clientSession), filename, options), destination);
    }

    @Override
    public GridFSFindIterable find() {
        throw new UnsupportedOperationException();
    }

    @Override
    public GridFSFindIterable find(final Bson filter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public GridFSFindIterable find(final ClientSession clientSession) {
        throw new UnsupportedOperationException();
    }

    @Override
    public GridFSFindIterable find(final ClientSession clientSession, final Bson filter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void delete(final ObjectId id) {
        delete(new BsonObjectId(id));
    }

    @Override
    public void delete(final BsonValue id) {
        Mono.from(wrapped.delete(id)).contextWrite(CONTEXT).block(TIMEOUT_DURATION);
    }

    @Override
    public void delete(final ClientSession clientSession, final ObjectId id) {
        delete(clientSession, new BsonObjectId(id));
    }

    @Override
    public void delete(final ClientSession clientSession, final BsonValue id) {
        Mono.from(wrapped.delete(unwrap(clientSession), id)).contextWrite(CONTEXT).block(TIMEOUT_DURATION);
    }

    @Override
    public void rename(final ObjectId id, final String newFilename) {
        rename(new BsonObjectId(id), newFilename);
    }

    @Override
    public void rename(final BsonValue id, final String newFilename) {
        Mono.from(wrapped.rename(id, newFilename)).contextWrite(CONTEXT).block(TIMEOUT_DURATION);
    }

    @Override
    public void rename(final ClientSession clientSession, final ObjectId id, final String newFilename) {
        rename(clientSession, new BsonObjectId(id), newFilename);
    }

    @Override
    public void rename(final ClientSession clientSession, final BsonValue id, final String newFilename) {
        Mono.from(wrapped.rename(unwrap(clientSession), id, newFilename)).contextWrite(CONTEXT).block(TIMEOUT_DURATION);
    }

    @Override
    public void drop() {
        Mono.from(wrapped.drop()).contextWrite(CONTEXT).block(TIMEOUT_DURATION);
    }

    @Override
    public void drop(final ClientSession clientSession) {
        Mono.from(wrapped.drop(unwrap(clientSession))).contextWrite(CONTEXT).block(TIMEOUT_DURATION);
    }

    private void toOutputStream(final GridFSDownloadPublisher downloadPublisher, final OutputStream destination) {
        Flux.from(downloadPublisher).toStream().forEach(byteBuffer -> {
            try {
                byte[] bytes = new byte[byteBuffer.remaining()];
                byteBuffer.get(bytes);
                destination.write(bytes);
            } catch (IOException e) {
                throw new MongoGridFSException("IOException when reading from the OutputStream", e);
            }
        });
    }

    @SuppressWarnings("BlockingMethodInNonBlockingContext")
    private Flux<ByteBuffer> inputStreamToFlux(final InputStream source, final GridFSUploadOptions options) {
        List<ByteBuffer> byteBuffers = new ArrayList<>();
        int chunkSize = options.getChunkSizeBytes() == null ? wrapped.getChunkSizeBytes() : options.getChunkSizeBytes();
        byte[] buffer = new byte[chunkSize];
        try {
            int len;
            while ((len = source.read(buffer)) != -1) {
                byteBuffers.add(ByteBuffer.wrap(buffer, 0, len));
                buffer = new byte[chunkSize];
            }
            return Flux.fromIterable(byteBuffers);
        } catch (IOException e) {
            throw new MongoGridFSException("IOException when reading from the InputStream", e);
        }
    }

    private com.mongodb.reactivestreams.client.ClientSession unwrap(final ClientSession clientSession) {
        return ((SyncClientSession) clientSession).getWrapped();
    }
}
