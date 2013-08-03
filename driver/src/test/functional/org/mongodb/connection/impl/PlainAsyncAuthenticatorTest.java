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

package org.mongodb.connection.impl;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mongodb.CommandResult;
import org.mongodb.MongoCredential;
import org.mongodb.MongoException;
import org.mongodb.command.MongoCommandFailureException;
import org.mongodb.connection.AsyncConnection;
import org.mongodb.connection.ServerAddress;
import org.mongodb.connection.SingleResultCallback;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertTrue;
import static org.mongodb.Fixture.getBufferProvider;

@Ignore
public class PlainAsyncAuthenticatorTest {
    private AsyncConnection connection;
    private String userName;
    private String password;

    @Before
    public void setUp() throws Exception {
        String host = System.getProperty("org.mongodb.test.host");
        userName = System.getProperty("org.mongodb.test.userName");
        password = System.getProperty("org.mongodb.test.password");
        connection = new DefaultAsyncConnection(new ServerAddress(host), new PowerOfTwoBufferPool());
    }

    @After
    public void tearDown() throws Exception {
        connection.close();
    }

    @Test
    public void testSuccessfulAuthentication() throws InterruptedException {
        PlainAsyncAuthenticator authenticator = new PlainAsyncAuthenticator(
                MongoCredential.createPlainCredential(userName, password.toCharArray()), connection, getBufferProvider());
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Boolean> gotResult = new AtomicReference<Boolean>(false);
        authenticator.authenticate(new SingleResultCallback<CommandResult>() {
            @Override
            public void onResult(final CommandResult result, final MongoException e) {
                gotResult.set(e == null && result != null && result.isOk());
                latch.countDown();
            }
        });
        latch.await(10, TimeUnit.SECONDS);
        assertTrue(gotResult.get());
    }

    @Test
    public void testUnsuccessfulAuthentication() throws InterruptedException {
        PlainAsyncAuthenticator authenticator = new PlainAsyncAuthenticator(
                MongoCredential.createPlainCredential(userName, "wrong".toCharArray()), connection, getBufferProvider());
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Boolean> gotException = new AtomicReference<Boolean>(false);
        authenticator.authenticate(new SingleResultCallback<CommandResult>() {
            @Override
            public void onResult(final CommandResult result, final MongoException e) {
                gotException.set(result == null && e != null && e instanceof MongoCommandFailureException);
                latch.countDown();
            }
        });
        latch.await(10, TimeUnit.SECONDS);
        assertTrue(gotException.get());
    }
}
