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

package org.mongodb.impl;

import category.Async;
import category.Slow;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mongodb.DatabaseTestCase;
import org.mongodb.Document;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.operation.CountOperation;
import org.mongodb.operation.Find;
import org.mongodb.operation.Insert;
import org.mongodb.operation.InsertOperation;
import org.mongodb.session.PinnedSession;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.mongodb.Fixture.getBufferProvider;
import static org.mongodb.Fixture.getCluster;
import static org.mongodb.Fixture.getExecutor;
import static org.mongodb.Fixture.getSession;
import static org.mongodb.WriteConcern.ACKNOWLEDGED;
import static org.mongodb.WriteConcern.UNACKNOWLEDGED;

@Category({Async.class, Slow.class})
public class AsyncBatchInsertTest extends DatabaseTestCase {

    private List<Document> documents;

    @Before
    public void setUp() {
        byte[] hugeByteArray = new byte[1024 * 1024 * 15];

        documents = new ArrayList<Document>();
        documents.add(new Document("bytes", hugeByteArray));
        documents.add(new Document("bytes", hugeByteArray));
        documents.add(new Document("bytes", hugeByteArray));
        documents.add(new Document("bytes", hugeByteArray));
        documents.add(new Document("bytes", hugeByteArray));
        documents.add(new Document("bytes", hugeByteArray));
        documents.add(new Document("bytes", hugeByteArray));
        documents.add(new Document("bytes", hugeByteArray));

        super.setUp();
    }

    @Test
    public void testBatchInsert() throws ExecutionException, InterruptedException {
        Insert<Document> insert = new Insert<Document>(ACKNOWLEDGED, documents);
        new InsertOperation<Document>(collection.getNamespace(),
                                      insert,
                                      new DocumentCodec(),
                                      getBufferProvider(),
                                      getSession(),
                                      true)
            .executeAsync().get();
        assertEquals(documents.size(), collection.find().count());
    }

    // To make the assertion work for unacknowledged writes, have to bind to a single connection
    @Test
    public void testUnacknowledgedBatchInsert() throws ExecutionException, InterruptedException {
        PinnedSession session = new PinnedSession(getCluster(), getExecutor());
        try {
            Insert<Document> insert = new Insert<Document>(UNACKNOWLEDGED, documents);
            InsertOperation<Document> insertOperation = new InsertOperation<Document>(collection.getNamespace(),
                                                                                      insert,
                                                                                      new DocumentCodec(),
                                                                                      getBufferProvider(),
                                                                                      session,
                                                                                      false);
            insertOperation.executeAsync().get();
            long count = new CountOperation(collection.getNamespace(), new Find(), new DocumentCodec(), getBufferProvider(), session, false)
                             .execute();
            assertEquals(documents.size(), count);
        } finally {
            session.close();
        }
    }
}
