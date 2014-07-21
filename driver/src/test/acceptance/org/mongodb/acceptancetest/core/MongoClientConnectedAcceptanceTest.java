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

package org.mongodb.acceptancetest.core;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.mongodb.client.Fixture.getOptions;
import static com.mongodb.client.Fixture.getPrimary;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class MongoClientConnectedAcceptanceTest {

    private MongoClient mongoClient;

    @Before
    public void setUp() throws Exception {
        mongoClient = MongoClients.create(getPrimary(), getOptions());
    }

    @After
    public void tearDown() {
        mongoClient.close();
    }

    @Test
    public void shouldBeConnectedToMongoAsSoonAsNewSingleServerMongoClientIsCreated() {

        double pingValue = mongoClient.tools().ping();

        assertThat(pingValue, is(1.0));
    }

    @Test(expected = IllegalStateException.class)
    public void shouldDisconnectFromServerWhenRequested() {
        mongoClient.close();

        mongoClient.tools().ping();
    }
}
