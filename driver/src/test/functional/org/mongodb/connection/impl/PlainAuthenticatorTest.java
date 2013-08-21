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
import org.mongodb.MongoCredential;
import org.mongodb.connection.Connection;
import org.mongodb.connection.MongoSecurityException;
import org.mongodb.connection.ServerAddress;

import static org.mongodb.Fixture.getBufferProvider;

@Ignore
public class PlainAuthenticatorTest {
    private Connection connection;
    private String userName;
    private String source;
    private String password;

    @Before
    public void setUp() throws Exception {
        String host = System.getProperty("org.mongodb.test.host");
        userName = System.getProperty("org.mongodb.test.userName");
        source = System.getProperty("org.mongod.test.source");
        password = System.getProperty("org.mongodb.test.password");
        connection = new DefaultSocketChannelConnection(new ServerAddress(host), ConnectionSettings.builder().build(),
                new PowerOfTwoBufferPool());
    }

    @After
    public void tearDown() throws Exception {
        connection.close();
    }

    @Test
    public void testSuccessfulAuthentication() {
        PlainAuthenticator authenticator = new PlainAuthenticator(
                MongoCredential.createPlainCredential(userName, source, password.toCharArray()), connection, getBufferProvider());
        authenticator.authenticate();
    }

    @Test(expected = MongoSecurityException.class)
    public void testUnsuccessfulAuthentication() {
        PlainAuthenticator authenticator = new PlainAuthenticator(
                MongoCredential.createPlainCredential(userName, source, "wrong".toCharArray()), connection, getBufferProvider());
        authenticator.authenticate();
    }
}
