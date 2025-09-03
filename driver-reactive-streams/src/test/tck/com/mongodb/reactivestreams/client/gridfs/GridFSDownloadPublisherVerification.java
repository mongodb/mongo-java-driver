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

package com.mongodb.reactivestreams.client.gridfs;

import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import com.mongodb.reactivestreams.client.MongoFixture;
import org.reactivestreams.Publisher;
import org.reactivestreams.tck.PublisherVerification;
import org.reactivestreams.tck.TestEnvironment;
import reactor.core.publisher.Flux;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static com.mongodb.reactivestreams.client.MongoFixture.DEFAULT_TIMEOUT_MILLIS;
import static com.mongodb.reactivestreams.client.MongoFixture.PUBLISHER_REFERENCE_CLEANUP_TIMEOUT_MILLIS;
import static com.mongodb.reactivestreams.client.MongoFixture.run;

public class GridFSDownloadPublisherVerification extends PublisherVerification<ByteBuffer> {

    public GridFSDownloadPublisherVerification() {
        super(new TestEnvironment(DEFAULT_TIMEOUT_MILLIS), PUBLISHER_REFERENCE_CLEANUP_TIMEOUT_MILLIS);
    }


    @Override
    public Publisher<ByteBuffer> createPublisher(final long elements) {
        assert (elements <= maxElementsFromPublisher());

        run(MongoFixture.getDefaultDatabase().drop());
        GridFSBucket bucket = GridFSBuckets.create(MongoFixture.getDefaultDatabase());

        if (elements < 1) {
            return bucket.downloadToPublisher("test");
        }

        List<ByteBuffer> byteBuffers = LongStream.rangeClosed(1, elements).boxed()
                .map(i -> ByteBuffer.wrap("test".getBytes())).collect(Collectors.toList());

        Publisher<ByteBuffer> uploader = Flux.fromIterable(byteBuffers);
        run(bucket.uploadFromPublisher("test", uploader, new GridFSUploadOptions().chunkSizeBytes(4)));

        return bucket.downloadToPublisher("test");
    }

    @Override
    public Publisher<ByteBuffer> createFailedPublisher() {
        return null;
    }

    @Override
    public long maxElementsFromPublisher() {
        return 100;
    }
}
