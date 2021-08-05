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

import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.reactivestreams.Publisher;
import org.reactivestreams.tck.PublisherVerification;
import org.reactivestreams.tck.TestEnvironment;

import static com.mongodb.reactivestreams.client.MongoFixture.DEFAULT_TIMEOUT_MILLIS;
import static com.mongodb.reactivestreams.client.MongoFixture.PUBLISHER_REFERENCE_CLEANUP_TIMEOUT_MILLIS;
import static com.mongodb.reactivestreams.client.MongoFixture.cleanDatabases;
import static com.mongodb.reactivestreams.client.MongoFixture.getDefaultDatabaseName;
import static com.mongodb.reactivestreams.client.MongoFixture.getMongoClient;
import static com.mongodb.reactivestreams.client.MongoFixture.run;
import static java.lang.String.format;

public class ListDatabasesPublisherVerification extends PublisherVerification<Document> {

    public ListDatabasesPublisherVerification() {
        super(new TestEnvironment(DEFAULT_TIMEOUT_MILLIS), PUBLISHER_REFERENCE_CLEANUP_TIMEOUT_MILLIS);
    }


    @Override
    public Publisher<Document> createPublisher(final long elements) {
        assert (elements <= maxElementsFromPublisher());

        cleanDatabases();
        MongoClient client = getMongoClient();
        for (long i = 0; i < elements; i++) {
            run(client.getDatabase(getDefaultDatabaseName() + i).createCollection("test" + i));
        }
        return client.listDatabases().filter(Filters.regex("name", format("^%s.*", getDefaultDatabaseName())));
    }

    @Override
    public Publisher<Document> createFailedPublisher() {
        return null;
    }

    @Override
    public long maxElementsFromPublisher() {
        return 5;
    }
}
