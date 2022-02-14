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

package com.mongodb.internal.connection;

import com.mongodb.MongoCompressor;
import com.mongodb.MongoCredential;
import com.mongodb.MongoSecurityException;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerId;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SocketStreamFactory;
import com.mongodb.connection.StreamFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Collections;

import static com.mongodb.ClusterFixture.getClusterConnectionMode;
import static com.mongodb.ClusterFixture.getServerApi;
import static com.mongodb.ClusterFixture.getSslSettings;

@Ignore
public class PlainAuthenticatorTest {
    private InternalConnection internalConnection;
    private ConnectionDescription connectionDescription;
    private String userName;
    private String source;
    private String password;
    private StreamFactory streamFactory = new SocketStreamFactory(SocketSettings.builder().build(), getSslSettings());

    @Before
    public void setUp() {
        String host = System.getProperty("org.mongodb.test.host");
        userName = System.getProperty("org.mongodb.test.userName");
        source = System.getProperty("org.mongod.test.source");
        password = System.getProperty("org.mongodb.test.password");
        internalConnection = new InternalStreamConnectionFactory(ClusterConnectionMode.SINGLE, streamFactory, null, null,
                null, Collections.<MongoCompressor>emptyList(), null, getServerApi()).create(new ServerId(new ClusterId(),
                new ServerAddress(host)));
        connectionDescription = new ConnectionDescription(new ServerId(new ClusterId(), new ServerAddress()));
    }

    @After
    public void tearDown() {
        internalConnection.close();
    }

    @Test
    public void testSuccessfulAuthentication() {
        PlainAuthenticator authenticator = new PlainAuthenticator(getCredentialWithCache(userName, source, password.toCharArray()),
                getClusterConnectionMode(), getServerApi());
        authenticator.authenticate(internalConnection, connectionDescription);
    }

    @Test(expected = MongoSecurityException.class)
    public void testUnsuccessfulAuthentication() {
        PlainAuthenticator authenticator = new PlainAuthenticator(getCredentialWithCache(userName, source, "wrong".toCharArray()),
                getClusterConnectionMode(), getServerApi());
        authenticator.authenticate(internalConnection, connectionDescription);
    }

    private static MongoCredentialWithCache getCredentialWithCache(final String userName, final String source, final char[] password) {
        return new MongoCredentialWithCache(MongoCredential.createPlainCredential(userName, source, password));
    }
}
