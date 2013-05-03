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

package org.mongodb;

import category.Slow;
import org.bson.types.BSONTimestamp;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mongodb.command.MongoCommand;
import org.mongodb.operation.MongoFind;
import org.mongodb.operation.QueryOption;
import org.mongodb.result.CommandResult;

import java.util.EnumSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MongoQueryCursorTest extends DatabaseTestCase {
    private MongoQueryCursor<Document> cursor;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        for (int i = 0; i < 10; i++) {
            collection.insert(new Document("_id", i));
        }
    }

    @After
    public void tearDown() {
        super.tearDown();
        if (cursor != null) {
            cursor.close();
        }
    }

    @Test
    public void testSizesAndNumGetMores() {
        cursor = new MongoQueryCursor<Document>(collection.getNamespace(), new MongoFind().batchSize(2),
                collection.getOptions().getDocumentCodec(), collection.getCodec(), Fixture.getMongoConnector());
        assertEquals(0, cursor.getNumGetMores());
        assertEquals(1, cursor.getSizes().size());
        assertEquals(2, (int) cursor.getSizes().get(0));
        cursor.next();
        cursor.next();
        cursor.next();
        assertEquals(1, cursor.getNumGetMores());
        assertEquals(2, cursor.getSizes().size());
        assertEquals(2, (int) cursor.getSizes().get(1));
        cursor.next();
        cursor.next();
        assertEquals(2, cursor.getNumGetMores());
        assertEquals(3, cursor.getSizes().size());
        assertEquals(2, (int) cursor.getSizes().get(2));
    }

    @Test
    @Category(Slow.class)
    public void testTailableAwait() {
        collection.tools().drop();
        database.tools().createCollection(new CreateCollectionOptions(collectionName, true, 1000));

        collection.insert(new Document("_id", 1).append("ts", new BSONTimestamp(5, 0)));

        cursor = new MongoQueryCursor<Document>(collection.getNamespace(), new MongoFind()
                .filter(new Document("ts", new Document("$gte", new BSONTimestamp(5, 0))))
                .batchSize(2)
                .addOptions(EnumSet.of(QueryOption.Tailable, QueryOption.AwaitData)),
                collection.getOptions().getDocumentCodec(), collection.getCodec(), Fixture.getMongoConnector());
        assertTrue(cursor.hasNext());
        assertEquals(1, cursor.next().get("_id"));
        assertTrue(cursor.hasNext());

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(5000);
                    collection.insert(new Document("_id", 2).append("ts", new BSONTimestamp(6, 0)));
                } catch (InterruptedException e) { // NOPMD
                    // all good
                }
            }
        }).start();

        // Note: this test is racy.  There is no guarantee that we're testing what we're trying to, which is the loop in the next() method.
        assertEquals(2, cursor.next().get("_id"));
    }

//    @Test
//    @Category(Slow.class)
//    public void testTailableAwaitInterrupt() throws InterruptedException {
//        collection.tools().drop();
//        database.tools().createCollection(new CreateCollectionOptions(collectionName, true, 1000));
//
//        collection.insert(new Document("_id", 1));
//
//        cursor = new MongoQueryCursor<Document>(collection.getNamespace(), new MongoFind()
//                .batchSize(2)
//                .addOptions(EnumSet.of(QueryOption.Tailable, QueryOption.AwaitData)),
//                collection.getOptions().getDocumentCodec(), collection.getCodec(), Fixture.getMongoConnector());
//
//        final CountDownLatch latch = new CountDownLatch(1);
//        Thread t = new Thread(new Runnable() {
//            @Override
//            public void run() {
//                try {
//                    cursor.next();
//                    cursor.next();
//                    fail();
//                } catch (MongoInterruptedException e) {
//                    latch.countDown();
//                }
//            }
//        });
//        t.start();
//        Thread.sleep(1000);  // Note: this is racy, as where the interrupted exception is actually thrown from depends on timing.
//        t.interrupt();
//        latch.await();
//    }

    @Test
    @Ignore("Won't work with replica sets or when tests are run in parallel")
    public void shouldKillCursorIfLimitIsReachedOnInitialQuery() {
        int openCursors = getTotalOpenCursors();
        cursor = new MongoQueryCursor<Document>(collection.getNamespace(), new MongoFind().limit(5),
                collection.getOptions().getDocumentCodec(), collection.getCodec(), Fixture.getMongoConnector());
        assertEquals(openCursors, getTotalOpenCursors());
    }

    @Test
    @Ignore("Won't work with replica sets or when tests are run in parallel")
    public void shouldKillCursorIfLimitIsReachedOnGetMore() {
        int openCursors = getTotalOpenCursors();
        cursor = new MongoQueryCursor<Document>(collection.getNamespace(), new MongoFind().batchSize(3).limit(5),
                collection.getOptions().getDocumentCodec(), collection.getCodec(), Fixture.getMongoConnector());
        cursor.next();
        cursor.next();
        cursor.next();
        cursor.next();

        assertEquals(openCursors, getTotalOpenCursors());
    }

    private int getTotalOpenCursors() {
        CommandResult commandResult = Fixture.getMongoClient().getDatabase("admin").executeCommand(new MongoCommand(
                new Document("serverStatus", 1)));
        return (Integer) ((Document) commandResult.getResponse().get("cursors")).get("totalOpen");
    }
}
