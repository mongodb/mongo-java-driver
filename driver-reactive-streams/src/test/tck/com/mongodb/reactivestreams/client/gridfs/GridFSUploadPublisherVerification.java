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

import com.mongodb.reactivestreams.client.MongoFixture;
import org.bson.types.ObjectId;
import org.reactivestreams.Publisher;
import org.reactivestreams.tck.PublisherVerification;
import org.reactivestreams.tck.TestEnvironment;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static com.mongodb.reactivestreams.client.MongoFixture.DEFAULT_TIMEOUT_MILLIS;
import static com.mongodb.reactivestreams.client.MongoFixture.PUBLISHER_REFERENCE_CLEANUP_TIMEOUT_MILLIS;
import static com.mongodb.reactivestreams.client.MongoFixture.run;
import static com.mongodb.reactivestreams.client.internal.Publishers.publishAndFlatten;

public class GridFSUploadPublisherVerification extends PublisherVerification<ObjectId> {

    public GridFSUploadPublisherVerification() {
        super(new TestEnvironment(DEFAULT_TIMEOUT_MILLIS), PUBLISHER_REFERENCE_CLEANUP_TIMEOUT_MILLIS);
    }


    @Override
    public Publisher<ObjectId> createPublisher(final long elements) {
        assert (elements <= maxElementsFromPublisher());

        if (elements < 1) {
            notVerified();
        }

        GridFSBucket bucket = GridFSBuckets.create(MongoFixture.getDefaultDatabase());

        run(MongoFixture.getDefaultDatabase().drop());
        List<ByteBuffer> byteBuffers = LongStream.rangeClosed(1, elements).boxed()
                .map(i -> ByteBuffer.wrap("test".getBytes())).collect(Collectors.toList());

        Publisher<ByteBuffer> uploader = publishAndFlatten(callback -> callback.onResult(byteBuffers, null));

        return bucket.uploadFromPublisher("test", uploader);
    }

    @Override
    public Publisher<ObjectId> createFailedPublisher() {
        return null;
    }

    @Override
    public long maxElementsFromPublisher() {
        return 1;
    }
}
