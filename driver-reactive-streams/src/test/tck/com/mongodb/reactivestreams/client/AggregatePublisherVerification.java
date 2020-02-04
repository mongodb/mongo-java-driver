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

package com.mongodb.reactivestreams.client;

import org.bson.Document;
import org.reactivestreams.Publisher;
import org.reactivestreams.tck.PublisherVerification;
import org.reactivestreams.tck.TestEnvironment;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static com.mongodb.reactivestreams.client.MongoFixture.DEFAULT_TIMEOUT_MILLIS;
import static com.mongodb.reactivestreams.client.MongoFixture.PUBLISHER_REFERENCE_CLEANUP_TIMEOUT_MILLIS;
import static com.mongodb.reactivestreams.client.MongoFixture.run;

public class AggregatePublisherVerification extends PublisherVerification<Document> {

    public AggregatePublisherVerification() {
        super(new TestEnvironment(DEFAULT_TIMEOUT_MILLIS), PUBLISHER_REFERENCE_CLEANUP_TIMEOUT_MILLIS);
    }


    @Override
    public Publisher<Document> createPublisher(final long elements) {
        assert (elements <= maxElementsFromPublisher());

        MongoCollection<Document> collection = MongoFixture.getDefaultDatabase().getCollection("AggregationTest");
        run(collection.drop());
        if (elements > 0) {
            List<Document> documentList = LongStream.rangeClosed(1, elements).boxed()
                    .map(i -> new Document("a", i)).collect(Collectors.toList());

            run(collection.insertMany(documentList));
        }

        return collection.aggregate(Collections.singletonList(Document.parse("{$match: {}}")));
    }

    @Override
    public Publisher<Document> createFailedPublisher() {
        return null;
    }

    @Override
    public long maxElementsFromPublisher() {
        return 100;
    }
}
