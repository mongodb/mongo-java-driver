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
import org.mongodb.MongoClient;
import org.mongodb.MongoCollection;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.mongodb.Fixture.getMongoClient;
import static org.mongodb.WriteConcern.ACKNOWLEDGED;
import static org.mongodb.WriteConcern.UNACKNOWLEDGED;


@Category({Async.class, Slow.class})
public class AsyncBatchInsertTest extends DatabaseTestCase {

    private List<Document> insertDocumentList;

    @Before
    public void setUp() {
        byte[] hugeByteArray = new byte[1024 * 100];

        insertDocumentList = new ArrayList<Document>();
        insertDocumentList.add(new Document("bytes", hugeByteArray));
        insertDocumentList.add(new Document("bytes", hugeByteArray));
        insertDocumentList.add(new Document("bytes", hugeByteArray));
        insertDocumentList.add(new Document("bytes", hugeByteArray));
        insertDocumentList.add(new Document("bytes", hugeByteArray));
        insertDocumentList.add(new Document("bytes", hugeByteArray));
        insertDocumentList.add(new Document("bytes", hugeByteArray));
        insertDocumentList.add(new Document("bytes", hugeByteArray));

        super.setUp();
    }

    @Test
    public void testBatchInsert() throws ExecutionException, InterruptedException {
        collection.withWriteConcern(ACKNOWLEDGED).asyncInsert(insertDocumentList).get();
        assertEquals(insertDocumentList.size(), collection.find().count());
    }

    @Test
    public void testUnacknowledgedBatchInsert() throws ExecutionException, InterruptedException {
        final MongoClient client = getMongoClient();
        Long count = client.withConnection(new Callable<Long>() {
            public Long call() {
                MongoCollection<Document> collection = client.getDatabase(getDatabaseName()).getCollection(getCollectionName());
                collection.withWriteConcern(UNACKNOWLEDGED).asyncInsert(insertDocumentList).get();
                return collection.find().asyncCount().get();
            }
        });
        assertEquals(count.intValue(), insertDocumentList.size());
    }
}
