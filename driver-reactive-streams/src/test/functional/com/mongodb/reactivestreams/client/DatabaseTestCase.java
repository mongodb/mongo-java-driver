/*
 * Copyright 2016 MongoDB, Inc.
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

package com.mongodb.reactivestreams.client;

import com.mongodb.MongoException;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.reactivestreams.Publisher;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.reactivestreams.client.Fixture.ObservableSubscriber;
import static com.mongodb.reactivestreams.client.Fixture.drop;
import static com.mongodb.reactivestreams.client.Fixture.getDefaultDatabaseName;
import static com.mongodb.reactivestreams.client.Fixture.getMongoClient;

public class DatabaseTestCase {
    //For ease of use and readability, in this specific case we'll allow protected variables
    //CHECKSTYLE:OFF
    protected MongoClient client;
    protected MongoDatabase database;
    protected MongoCollection<Document> collection;
    //CHECKSTYLE:ON

    @Before
    public void setUp() throws Throwable {
        client =  getMongoClient();
        database = client.getDatabase(getDefaultDatabaseName());
        collection = database.getCollection(getClass().getName());
        drop(collection.getNamespace());
    }

    @After
    public void tearDown() throws Throwable {
        if (collection != null) {
            drop(collection.getNamespace());
        }
    }

    public abstract class MongoOperation<TResult> {
        private final ObservableSubscriber<TResult> subscriber = new ObservableSubscriber<TResult>();

        public ObservableSubscriber<TResult> getSubscriber() {
            return subscriber;
        }

        public List<TResult> get() {
            execute();
            try {
                return subscriber.get(60, TimeUnit.SECONDS);
            } catch (MongoException e) {
                throw e;
            } catch (Throwable t) {
                throw new RuntimeException(t);
            }
        }

        public TResult head() {
            return get().get(0);
        }

        public abstract Publisher<TResult> execute();
    }
}
