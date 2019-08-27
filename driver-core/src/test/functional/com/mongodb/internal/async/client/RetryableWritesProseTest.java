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

package com.mongodb.internal.async.client;

import com.mongodb.MongoClientException;
import com.mongodb.MongoException;
import com.mongodb.async.FutureResultCallback;
import com.mongodb.client.test.CollectionHelper;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static com.mongodb.ClusterFixture.getServerStatus;
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.ClusterFixture.isSharded;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.ClusterFixture.serverVersionLessThan;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class RetryableWritesProseTest extends DatabaseTestCase {
    private CollectionHelper<Document> collectionHelper;

    @Before
    @Override
    public void setUp() {
        assumeTrue(canRunTests());
        super.setUp();

        collectionHelper = new CollectionHelper<Document>(new DocumentCodec(), collection.getNamespace());
        collectionHelper.create();
    }

    @Test
    public void testRetryWritesWithInsertOneAgainstMMAPv1RaisesError() {
        boolean exceptionFound = false;

        try {
            FutureResultCallback<Void> callback = new FutureResultCallback<Void>();
            collection.insertOne(Document.parse("{ x : 1 }"), callback);
            futureResult(callback);
        } catch (MongoClientException e) {
            assertTrue(e.getMessage().equals("This MongoDB deployment does not support retryable writes. "
                    + "Please add retryWrites=false to your connection string."));
            assertTrue(((MongoException) e.getCause()).getCode() == 20);
            assertTrue(e.getCause().getMessage().contains("Transaction numbers"));
            exceptionFound = true;
        }
        assertTrue(exceptionFound);
    }

    @Test
    public void testRetryWritesWithFindOneAndDeleteAgainstMMAPv1RaisesError() {
        boolean exceptionFound = false;

        try {
            FutureResultCallback<Document> callback = new FutureResultCallback<Document>();
            collection.findOneAndDelete(Document.parse("{ x : 1 }"), callback);
            futureResult(callback);
        } catch (MongoClientException e) {
            assertTrue(e.getMessage().equals("This MongoDB deployment does not support retryable writes. "
                    + "Please add retryWrites=false to your connection string."));
            assertTrue(((MongoException) e.getCause()).getCode() == 20);
            assertTrue(e.getCause().getMessage().contains("Transaction numbers"));
            exceptionFound = true;
        }
        assertTrue(exceptionFound);
    }

    private boolean canRunTests() {
        Document storageEngine = (Document) getServerStatus().get("storageEngine");

        return ((isSharded() || isDiscoverableReplicaSet())
                && storageEngine != null && storageEngine.get("name").equals("mmapv1")
                && serverVersionAtLeast(3, 6) && serverVersionLessThan(4, 1));
    }

    private <T> T futureResult(final FutureResultCallback<T> callback) {
        try {
            return callback.get(30, TimeUnit.SECONDS);
        } catch (Throwable t) {
            throw MongoException.fromThrowable(t);
        }
    }
}
