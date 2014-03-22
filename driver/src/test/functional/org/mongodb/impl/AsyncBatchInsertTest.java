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
import org.mongodb.operation.InsertOperation;
import org.mongodb.operation.InsertRequest;
import org.mongodb.session.PinnedSession;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.mongodb.Fixture.getCluster;
import static org.mongodb.Fixture.getExecutor;
import static org.mongodb.Fixture.getSession;
import static org.mongodb.WriteConcern.ACKNOWLEDGED;
import static org.mongodb.WriteConcern.UNACKNOWLEDGED;

@Category({Async.class, Slow.class})
public class AsyncBatchInsertTest extends DatabaseTestCase {

    private List<InsertRequest<Document>> insertRequestList;

    @Before
    public void setUp() {
        byte[] hugeByteArray = new byte[1024 * 1024 * 15];

        insertRequestList = new ArrayList<InsertRequest<Document>>();
        insertRequestList.add(new InsertRequest<Document>(new Document("bytes", hugeByteArray)));
        insertRequestList.add(new InsertRequest<Document>(new Document("bytes", hugeByteArray)));
        insertRequestList.add(new InsertRequest<Document>(new Document("bytes", hugeByteArray)));
        insertRequestList.add(new InsertRequest<Document>(new Document("bytes", hugeByteArray)));
        insertRequestList.add(new InsertRequest<Document>(new Document("bytes", hugeByteArray)));
        insertRequestList.add(new InsertRequest<Document>(new Document("bytes", hugeByteArray)));
        insertRequestList.add(new InsertRequest<Document>(new Document("bytes", hugeByteArray)));
        insertRequestList.add(new InsertRequest<Document>(new Document("bytes", hugeByteArray)));

        super.setUp();
    }

    @Test
    public void testBatchInsert() throws ExecutionException, InterruptedException {
        new InsertOperation<Document>(collection.getNamespace(), true, ACKNOWLEDGED,
                                      insertRequestList,
                                      new DocumentCodec(),
                                      getSession(), true)
        .executeAsync().get();
        assertEquals(insertRequestList.size(), collection.find().count());
    }

    // To make the assertion work for unacknowledged writes, have to bind to a single connection
    @Test
    public void testUnacknowledgedBatchInsert() throws ExecutionException, InterruptedException {
        PinnedSession session = new PinnedSession(getCluster(), getExecutor());
        try {
            InsertOperation<Document> insertOperation = new InsertOperation<Document>(collection.getNamespace(), true, UNACKNOWLEDGED,
                                                                                      insertRequestList,
                                                                                      new DocumentCodec(),
                                                                                      session, false);
            insertOperation.executeAsync().get();
            long count = new CountOperation(collection.getNamespace(), new Find(), new DocumentCodec(), session, false)
                         .execute();
            assertEquals(insertRequestList.size(), count);
        } finally {
            session.close();
        }
    }
}
