/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb.connection;


import com.mongodb.MongoCompressor;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Collections;

import static com.mongodb.ClusterFixture.getPrimary;
import static com.mongodb.ClusterFixture.getSslSettings;
import static java.util.Collections.singletonList;

public class AuthenticatingConnectionTest {
    private InternalConnection internalConnection;
    private String userName;
    private String password;
    private String source;
    private ServerAddress serverAddress;

    @Before
    public void setUp() throws Exception {
        userName = System.getProperty("org.mongodb.test.userName", "bob");
        password = System.getProperty("org.mongodb.test.password", "pwd123");
        source = System.getProperty("org.mongodb.test.source", "admin");
        serverAddress = new ServerAddress(System.getProperty("org.mongodb.test.serverAddress", getPrimary().toString()));

        InternalConnectionFactory internalConnectionFactory =
            new InternalStreamConnectionFactory(new SocketStreamFactory(SocketSettings.builder().build(), getSslSettings()),
                                                Collections.<MongoCredential>emptyList(), null, null,
                                                Collections.<MongoCompressor>emptyList(), null);
        internalConnection = internalConnectionFactory.create(new ServerId(new ClusterId(), serverAddress));
    }

    @After
    public void tearDown() throws Exception {
        internalConnection.close();
    }

    @Test
    @Ignore
    public void testMongoCRAuthentication() throws InterruptedException {
        InternalConnectionFactory internalConnectionFactory =
            new InternalStreamConnectionFactory(new SocketStreamFactory(SocketSettings.builder().build(), getSslSettings()),
                                                singletonList(MongoCredential.createMongoCRCredential(userName, source,
                                                        password.toCharArray())),
                                                null, null, Collections.<MongoCompressor>emptyList(),
                                                null);
        internalConnection = internalConnectionFactory.create(new ServerId(new ClusterId(), serverAddress));
    }

    @Test
    @Ignore
    public void testPlainAuthentication() throws InterruptedException {
        InternalConnectionFactory internalConnectionFactory =
            new InternalStreamConnectionFactory(new SocketStreamFactory(SocketSettings.builder().build(), getSslSettings()),
                                                singletonList(MongoCredential.createPlainCredential(userName, source,
                                                                                             password.toCharArray())),
                                                null, null, Collections.<MongoCompressor>emptyList(),
                                                null);
        internalConnection = internalConnectionFactory.create(new ServerId(new ClusterId(), serverAddress));
    }

    @Test
    @Ignore
    public void testGSSAPIAuthentication() throws InterruptedException {
        InternalConnectionFactory internalConnectionFactory =
            new InternalStreamConnectionFactory(new SocketStreamFactory(SocketSettings.builder().build(), getSslSettings()),
                                                singletonList(MongoCredential.createGSSAPICredential(userName)),
                                                null, null, Collections.<MongoCompressor>emptyList(),
                                                null);
        internalConnection = internalConnectionFactory.create(new ServerId(new ClusterId(), serverAddress));
    }

    @Test
    @Ignore
    public void testMongoX509Authentication() throws InterruptedException {
        InternalConnectionFactory internalConnectionFactory =
            new InternalStreamConnectionFactory(new SocketStreamFactory(SocketSettings.builder().build(),
                                                                        getSslSettings()),
                                                singletonList(MongoCredential.createMongoX509Credential("CN=client,OU=kerneluser,"
                                                                                                 + "O=10Gen,L=New York City,"
                                                                                                 + "ST=New York,C=US")),
                                                null, null, Collections.<MongoCompressor>emptyList(),
                                                null);
        internalConnection = internalConnectionFactory.create(new ServerId(new ClusterId(), serverAddress));

    }
}
