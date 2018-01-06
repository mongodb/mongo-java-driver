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
import com.mongodb.MongoSecurityException;
import com.mongodb.ServerAddress;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Collections;

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
    public void setUp() throws Exception {
        String host = System.getProperty("org.mongodb.test.host");
        userName = System.getProperty("org.mongodb.test.userName");
        source = System.getProperty("org.mongod.test.source");
        password = System.getProperty("org.mongodb.test.password");
        internalConnection = new InternalStreamConnectionFactory(streamFactory, Collections.<MongoCredential>emptyList(), null, null,
                                                                        Collections.<MongoCompressor>emptyList(), null)
                             .create(new ServerId(new ClusterId(), new ServerAddress(host)));
        connectionDescription = new ConnectionDescription(new ServerId(new ClusterId(), new ServerAddress()));
    }

    @After
    public void tearDown() throws Exception {
        internalConnection.close();
    }

    @Test
    public void testSuccessfulAuthentication() {
        PlainAuthenticator authenticator = new PlainAuthenticator(MongoCredential.createPlainCredential(userName, source,
                                                                                                        password.toCharArray())
        );
        authenticator.authenticate(internalConnection, connectionDescription);
    }

    @Test(expected = MongoSecurityException.class)
    public void testUnsuccessfulAuthentication() {
        PlainAuthenticator authenticator = new PlainAuthenticator(MongoCredential.createPlainCredential(userName, source,
                                                                                                        "wrong".toCharArray())
        );
        authenticator.authenticate(internalConnection, connectionDescription);
    }
}
