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

import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.reactivestreams.client.MongoFixture;
import org.reactivestreams.Publisher;
import org.reactivestreams.tck.PublisherVerification;
import org.reactivestreams.tck.TestEnvironment;
import reactor.core.publisher.Flux;

import java.nio.ByteBuffer;
import java.util.Collections;

import static com.mongodb.reactivestreams.client.MongoFixture.DEFAULT_TIMEOUT_MILLIS;
import static com.mongodb.reactivestreams.client.MongoFixture.PUBLISHER_REFERENCE_CLEANUP_TIMEOUT_MILLIS;
import static com.mongodb.reactivestreams.client.MongoFixture.run;

public class GridFSFindPublisherVerification extends PublisherVerification<GridFSFile> {

    public GridFSFindPublisherVerification() {
        super(new TestEnvironment(DEFAULT_TIMEOUT_MILLIS), PUBLISHER_REFERENCE_CLEANUP_TIMEOUT_MILLIS);
    }

    @Override
    public Publisher<GridFSFile> createPublisher(final long elements) {
        assert (elements <= maxElementsFromPublisher());

        run(MongoFixture.getDefaultDatabase().drop());
        GridFSBucket bucket = GridFSBuckets.create(MongoFixture.getDefaultDatabase());

        for (long i = 0; i < elements; i++) {
            run(GridFSBuckets.create(MongoFixture.getDefaultDatabase()).uploadFromPublisher("test" + i,
                    Flux.fromIterable(Collections.singletonList(ByteBuffer.wrap("test".getBytes())))));
        }

        return bucket.find();
    }

    @Override
    public Publisher<GridFSFile> createFailedPublisher() {
        return null;
    }

    @Override
    public long maxElementsFromPublisher() {
        return 100;
    }
}
