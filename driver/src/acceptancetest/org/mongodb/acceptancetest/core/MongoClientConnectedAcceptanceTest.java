/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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

package org.mongodb.acceptancetest.core;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mongodb.MongoClient;
import org.mongodb.ServerAddress;
import org.mongodb.impl.SingleServerMongoClient;

import java.net.UnknownHostException;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class MongoClientConnectedAcceptanceTest {

    private static final String SERVER_NAME = "localhost";
    private static final int PORT = 27017;
    private ServerAddress serverAddress;

    @Before
    public void setUp() throws UnknownHostException {
        serverAddress = new ServerAddress(SERVER_NAME, PORT);
    }

    @Test
    public void shouldBeConnectedToMongoAsSoonAsNewSingleServerMongoClientIsCreated() {
        final MongoClient mongoClient = new SingleServerMongoClient(serverAddress);

        final double pingValue = mongoClient.admin().ping();

        assertThat(pingValue, is(1.0));
    }

    @Test
    @Ignore("Not working at the moment because SingleServerMongoClient hasn't implemented close")
    public void shouldDisconnectFromServerWhenRequested() {
        final MongoClient mongoClient = new SingleServerMongoClient(serverAddress);

        mongoClient.close();

        final double pingValue = mongoClient.admin().ping();

        assertThat(pingValue, not(1.0));
    }
}
