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

package org.mongodb.session;

import category.ReplicaSet;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mongodb.DatabaseTestCase;
import org.mongodb.Document;
import org.mongodb.MongoNamespace;
import org.mongodb.ReadPreference;
import org.mongodb.WriteConcern;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.connection.ServerAddress;
import org.mongodb.operation.Find;
import org.mongodb.operation.Insert;
import org.mongodb.operation.InsertOperation;
import org.mongodb.operation.QueryOperation;

import static org.junit.Assert.assertEquals;
import static org.mongodb.Fixture.getBufferProvider;
import static org.mongodb.Fixture.getCluster;

@Category(ReplicaSet.class)
public class PinnedSessionTest extends DatabaseTestCase {
    private PinnedSession session;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        session = new PinnedSession(getCluster());
    }

    @Test
    public void shouldPinReadsToSameServer() throws InterruptedException {
        MongoNamespace namespace = collection.getNamespace();
        DocumentCodec codec = new DocumentCodec();
        Find find = new Find().readPreference(ReadPreference.secondary()).batchSize(-1);
        ServerAddress serverAddress = session.execute(new QueryOperation<Document>(collection.getNamespace(), find, codec, codec,
                getBufferProvider())).getAddress();

        // there is randomization in the selection, so have to try a bunch of times.
        for (int i = 0; i < 100; i++) {
            assertEquals(serverAddress, session.execute(new QueryOperation<Document>(namespace, find, codec, codec,
                    getBufferProvider())).getAddress());
        }

        session.execute(new InsertOperation<Document>(namespace,
                new Insert<Document>(new Document()).writeConcern(WriteConcern.ACKNOWLEDGED), codec, getBufferProvider()));

        assertEquals(serverAddress, session.execute(new QueryOperation<Document>(namespace, find, codec, codec,
                getBufferProvider())).getAddress());
    }
}
