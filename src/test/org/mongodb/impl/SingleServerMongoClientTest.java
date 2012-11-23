/**
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
 *
 */

package org.mongodb.impl;

import junit.framework.Assert;
import org.mongodb.CommandResult;
import org.mongodb.MongoDocument;
import org.mongodb.ServerAddress;
import org.mongodb.WriteConcern;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.net.UnknownHostException;

public class SingleServerMongoClientTest extends Assert {
    private SingleServerMongoClient mongoClient;

    @BeforeTest
    public void setUp() throws UnknownHostException {
        mongoClient = new SingleServerMongoClient(new ServerAddress());
    }

    @Test
    public void testCommandExecution() {
        MongoDocument cmd = new MongoDocument("count", "test");
        CommandResult res = mongoClient.executeCommand("test", cmd);
        assertNotNull(res);
        assertTrue(res.getMongoDocument().get("n") instanceof Double);
    }

    @Test
    public void testInsertion() {
        mongoClient.executeCommand("test", new MongoDocument("drop", "test"));
        MongoDocument doc = new MongoDocument();
        mongoClient.insert("test.test", doc, WriteConcern.NONE);
        CommandResult res = mongoClient.executeCommand("test", new MongoDocument("count", "test"));
        assertEquals(1.0, res.getMongoDocument().get("n"));
    }
}
