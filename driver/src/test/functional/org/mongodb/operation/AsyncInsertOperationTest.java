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

package org.mongodb.operation;

import category.Async;
import category.Slow;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mongodb.DatabaseTestCase;
import org.mongodb.Document;
import org.mongodb.codecs.DocumentCodec;
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
public class AsyncInsertOperationTest extends DatabaseTestCase {

    private List<InsertRequest<Document>> insertDocumentList;

    @Before
    public void setUp() {
        byte[] hugeByteArray = new byte[1024 * 100];

        insertDocumentList = new ArrayList<InsertRequest<Document>>();
        insertDocumentList.add(new InsertRequest<Document>(new Document("bytes", hugeByteArray)));
        insertDocumentList.add(new InsertRequest<Document>(new Document("bytes", hugeByteArray)));
        insertDocumentList.add(new InsertRequest<Document>(new Document("bytes", hugeByteArray)));
        insertDocumentList.add(new InsertRequest<Document>(new Document("bytes", hugeByteArray)));
        insertDocumentList.add(new InsertRequest<Document>(new Document("bytes", hugeByteArray)));
        insertDocumentList.add(new InsertRequest<Document>(new Document("bytes", hugeByteArray)));
        insertDocumentList.add(new InsertRequest<Document>(new Document("bytes", hugeByteArray)));
        insertDocumentList.add(new InsertRequest<Document>(new Document("bytes", hugeByteArray)));

        super.setUp();
    }

    @Test
    public void testBatchInsert() throws ExecutionException, InterruptedException {
        InsertOperation<Document> op = new InsertOperation<Document>(getNamespace(), true, ACKNOWLEDGED, insertDocumentList,
                                                                     new DocumentCodec());
        op.execute(getSession());
        assertEquals((long) insertDocumentList.size(), getCollectionHelper().count());
    }

    @Test
    public void testUnacknowledgedBatchInsert() throws ExecutionException, InterruptedException {
        PinnedSession session = new PinnedSession(getCluster(), getExecutor());
        try {
            InsertOperation<Document> op = new InsertOperation<Document>(getNamespace(), true, UNACKNOWLEDGED, insertDocumentList,
                                                                         new DocumentCodec());
            op.execute(session);
            assertEquals(insertDocumentList.size(), getCollectionHelper().count());
        } finally {
            session.close();
        }
    }
}