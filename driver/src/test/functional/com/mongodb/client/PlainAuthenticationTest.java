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

package com.mongodb.client;

import com.mongodb.MongoCommandException;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ServerAddress;
import org.bson.Document;
import org.junit.Before;
import org.junit.Test;

import static com.mongodb.AuthenticationMechanism.PLAIN;
import static com.mongodb.ClusterFixture.getConnectionString;
import static com.mongodb.ClusterFixture.getCredentialList;
import static com.mongodb.Fixture.getMongoClient;
import static com.mongodb.Fixture.getMongoClientURI;
import static com.mongodb.MongoCredential.createPlainCredential;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class PlainAuthenticationTest {
    @Before
    public void setUp() {
        assumeTrue(!getCredentialList().isEmpty() && getCredentialList().get(0).getAuthenticationMechanism() == PLAIN);
    }

    @Test(expected = MongoCommandException.class)
    public void testUnsuccessfulAuthorization() throws InterruptedException {
        MongoClient client = new MongoClient(getServerAddress(), MongoClientOptions.builder().maxWaitTime(1000).build());
        MongoCollection<Document> collection = client.getDatabase(getConnectionString().getDatabase()).getCollection("test");
        try {
            collection.count();
        } finally {
            client.close();
        }
    }

    @Test
    public void testSuccessfulAuthenticationAndAuthorization() {
        MongoCollection<Document> collection = getMongoClient().getDatabase(getConnectionString().getDatabase()).getCollection("test");
        assertTrue(collection.count() >= 0); // Really just asserting that the query doesn't throw any security-related exceptions
    }

    @Test(expected = MongoTimeoutException.class)
    public void testUnsuccessfulAuthentication() throws InterruptedException {
        MongoClient client = new MongoClient(getServerAddress(), asList(createPlainCredential("wrongUserName", "$external",
                                                                                              "wrongPassword".toCharArray())),
                                             MongoClientOptions.builder().maxWaitTime(1000).build());
        MongoCollection<Document> collection = client.getDatabase(getConnectionString().getDatabase()).getCollection("test");
        try {
            collection.count();
        } finally {
            client.close();
        }
    }

    private ServerAddress getServerAddress() {
        return new ServerAddress(getMongoClientURI().getHosts().get(0));
    }
}
