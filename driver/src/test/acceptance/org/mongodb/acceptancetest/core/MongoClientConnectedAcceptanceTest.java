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

package org.mongodb.acceptancetest.core;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mongodb.MongoClient;
import org.mongodb.MongoClients;

import java.net.UnknownHostException;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mongodb.Fixture.getOptions;
import static org.mongodb.Fixture.getPrimary;

public class MongoClientConnectedAcceptanceTest {

    private MongoClient mongoClient;

    @Before
    public void setUp() throws UnknownHostException {
        mongoClient = MongoClients.create(getPrimary(), getOptions());
    }

    @After
    public void tearDown() {
        mongoClient.close();
    }

    @Test
    public void shouldBeConnectedToMongoAsSoonAsNewSingleServerMongoClientIsCreated() {

        final double pingValue = mongoClient.tools().ping();

        assertThat(pingValue, is(1.0));
    }

    @Test(expected = IllegalStateException.class)
    public void shouldDisconnectFromServerWhenRequested() {
        mongoClient.close();

        final double pingValue = mongoClient.tools().ping();

        assertThat(pingValue, not(1.0));
    }
}
