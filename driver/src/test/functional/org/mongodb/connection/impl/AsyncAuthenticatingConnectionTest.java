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

import org.bson.ByteBuf;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mongodb.Document;
import org.mongodb.MongoCredential;
import org.mongodb.MongoNamespace;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.connection.AsyncConnection;
import org.mongodb.connection.AsyncServerConnection;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.connection.ResponseSettings;
import org.mongodb.connection.ServerAddress;
import org.mongodb.connection.ServerDescription;
import org.mongodb.connection.SingleResultCallback;
import org.mongodb.operation.AsyncQueryOperation;
import org.mongodb.operation.Find;

import java.util.Arrays;
import java.util.List;

import static org.mongodb.Fixture.getBufferProvider;
import static org.mongodb.connection.ServerConnectionState.Connected;

public class AsyncAuthenticatingConnectionTest {
    private AsyncConnection connection;
    private String userName;
    private String password;
    private String source;
    private AsyncQueryOperation<Document> operation = new AsyncQueryOperation<Document>(new MongoNamespace("test", "test"), new Find(),
            new DocumentCodec(), new DocumentCodec(), getBufferProvider());

    @Before
    public void setUp() throws Exception {
        String host = System.getProperty("org.mongodb.test.host");
        userName = System.getProperty("org.mongodb.test.userName", "bob");
        password = System.getProperty("org.mongodb.test.password", "pwd123");
        source = System.getProperty("org.mongodb.test.source", "admin");
        connection = new DefaultAsyncConnection(new ServerAddress(host), new PowerOfTwoBufferPool());
    }

    @After
    public void tearDown() throws Exception {
        connection.close();
    }

    @Test
    @Ignore
    public void testMongoCRAuthentication() {
        final AuthenticatingAsyncConnection authenticatingConnection = new AuthenticatingAsyncConnection(connection,
                Arrays.asList(MongoCredential.createMongoCRCredential(userName, source, password.toCharArray())), getBufferProvider());
        operation.execute(new TestAsyncServerConnection(authenticatingConnection)).get();
    }

    @Test
    @Ignore
    public void testPlainAuthentication() {
        final AuthenticatingAsyncConnection authenticatingConnection = new AuthenticatingAsyncConnection(connection,
                Arrays.asList(MongoCredential.createPlainCredential(userName, password.toCharArray())), getBufferProvider());
        operation.execute(new TestAsyncServerConnection(authenticatingConnection)).get();
    }

    @Test
    @Ignore
    public void testGSSAPIAuthentication() {
        final AuthenticatingAsyncConnection authenticatingConnection = new AuthenticatingAsyncConnection(connection,
                Arrays.asList(MongoCredential.createGSSAPICredential(userName)), getBufferProvider());
        operation.execute(new TestAsyncServerConnection(authenticatingConnection)).get();
    }

    private static final class TestAsyncServerConnection implements AsyncServerConnection {

        private AsyncConnection wrapped;

        public TestAsyncServerConnection(final AsyncConnection connection) {
            wrapped = connection;
        }

        /**
         * Closes the connection.
         */
        @Override
        public void close() {
            wrapped.close();
        }

        /**
         * Returns the closed state of the connection
         *
         * @return true if connection is closed
         */
        @Override
        public boolean isClosed() {
            return wrapped.isClosed();
        }

        /**
         * Gets the server address of this connection
         */
        @Override
        public ServerAddress getServerAddress() {
            return wrapped.getServerAddress();
        }

        /**
         * Gets the description of the server that this is a connection to.
         *
         * @return the server description
         */
        @Override
        public ServerDescription getDescription() {
            return ServerDescription.builder().address(wrapped.getServerAddress()).state(Connected).build();
        }

        @Override
        public void sendMessage(final List<ByteBuf> byteBuffers, final SingleResultCallback<Void> callback) {
            wrapped.sendMessage(byteBuffers, callback);
        }

        @Override
        public void receiveMessage(final ResponseSettings responseSettings, final SingleResultCallback<ResponseBuffers> callback) {
            wrapped.receiveMessage(responseSettings, callback);
        }
    }
}
