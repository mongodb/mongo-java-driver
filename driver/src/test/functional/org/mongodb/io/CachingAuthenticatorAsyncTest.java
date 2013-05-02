/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb.io;

import category.Async;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mongodb.DatabaseTestCase;
import org.mongodb.MongoClientOptions;
import org.mongodb.MongoCredential;
import org.mongodb.MongoException;
import org.mongodb.async.SingleResultCallback;
import org.mongodb.command.MongoCommandFailureException;
import org.mongodb.impl.MongoAsyncConnection;
import org.mongodb.impl.MongoConnection;
import org.mongodb.impl.MongoCredentialsStore;
import org.mongodb.pool.SimplePool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

@Category(Async.class)
public class CachingAuthenticatorAsyncTest extends DatabaseTestCase {

    private CountDownLatch latch;
    private MongoConnection poolableConnector;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        latch = new CountDownLatch(1);
        poolableConnector = new MongoAsyncConnection(connector.getServerAddressList().get(0), null,
                new SimplePool<MongoConnection>("test", 1) {
            @Override
            protected MongoConnection createNew() {
                throw new UnsupportedOperationException();
            }
        }, new PowerOfTwoByteBufferPool(), MongoClientOptions.builder().build());
    }

    @After
    public void tearDown() {
        poolableConnector.close();
    }

    @Test
    public void testEmpty() throws InterruptedException {
        MongoCredentialsStore credentialsStore = new MongoCredentialsStore();
        CachingAuthenticator cachingAuthenticator = new CachingAuthenticator(credentialsStore, poolableConnector);

        final List<Exception> exceptionList = new ArrayList<Exception>();
        cachingAuthenticator.asyncAuthenticateAll(new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final MongoException e) {
                exceptionList.add(e);
                latch.countDown();
            }
        });

        latch.await();
        assertThat(exceptionList.get(0), is(nullValue()));
    }

    @Test
    public void testException() throws InterruptedException {
        MongoCredentialsStore credentialsStore =
                new MongoCredentialsStore(MongoCredential.createMongoCRCredential("noone", "nowhere", "nothing".toCharArray()));
        CachingAuthenticator cachingAuthenticator = new CachingAuthenticator(credentialsStore, poolableConnector);

        final List<Exception> exceptionList = new ArrayList<Exception>();
        cachingAuthenticator.asyncAuthenticateAll(new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final MongoException e) {
                exceptionList.add(e);
                latch.countDown();
            }
        });

        latch.await();
        assertThat(exceptionList.get(0), is(instanceOf(MongoCommandFailureException.class)));
    }
}
