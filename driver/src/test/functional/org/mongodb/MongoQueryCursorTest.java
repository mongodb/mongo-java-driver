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
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mongodb.operation.Find;
import org.mongodb.operation.GetMore;
import org.mongodb.operation.GetMoreOperation;
import org.mongodb.operation.KillCursor;
import org.mongodb.operation.MongoCursorNotFoundException;
import org.mongodb.operation.QueryFlag;
import org.mongodb.operation.protocol.KillCursorProtocolOperation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mongodb.Fixture.getBufferProvider;
import static org.mongodb.Fixture.getSession;

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
        if (cursor != null) {
            cursor.close();
        }
        super.tearDown();
    }

    @Test
    public void testServerCursor() {
        cursor = new MongoQueryCursor<Document>(collection.getNamespace(), new Find().batchSize(2),
                collection.getOptions().getDocumentCodec(), collection.getCodec(), getBufferProvider(), getSession(), false);
        assertNotNull(cursor.getServerCursor());
    }

    @Test
    public void testServerAddress() {
        cursor = new MongoQueryCursor<Document>(collection.getNamespace(), new Find(),
                collection.getOptions().getDocumentCodec(), collection.getCodec(), getBufferProvider(), getSession(), false);
        assertNull(cursor.getServerCursor());
        assertNotNull(cursor.getServerAddress());
    }

    @Test
    public void testGetCriteria() {
        final Find find = new Find().batchSize(2);
        cursor = new MongoQueryCursor<Document>(collection.getNamespace(), find,
                collection.getOptions().getDocumentCodec(), collection.getCodec(), getBufferProvider(), getSession(), false);
        assertEquals(find, cursor.getCriteria());
    }

    @Test
    public void testClosedState() {
        cursor = new MongoQueryCursor<Document>(collection.getNamespace(), new Find(),
                collection.getOptions().getDocumentCodec(), collection.getCodec(), getBufferProvider(), getSession(), false);
        cursor.close();
        cursor.close();
        try {
            cursor.next();
            fail();
        } catch (IllegalStateException e) { // NOPMD
            // all good
        }

        try {
            cursor.hasNext();
            fail();
        } catch (IllegalStateException e) {  // NOPMD
            // all good
        }

        try {
            cursor.getServerCursor();
            fail();
        } catch (IllegalStateException e) {  // NOPMD
            // all good
        }
    }

    @Test
    public void testGoingPastTheEnd() {
        cursor = new MongoQueryCursor<Document>(collection.getNamespace(), new Find().limit(2),
                collection.getOptions().getDocumentCodec(), collection.getCodec(), getBufferProvider(), getSession(), false);
        cursor.next();
        cursor.next();
        try {
            cursor.next();
            fail();
        } catch (NoSuchElementException e) { // NOPMD
            // all good
        }
    }

    @Test
    public void testNormalExhaustion() {
        cursor = new MongoQueryCursor<Document>(collection.getNamespace(), new Find(),
                collection.getOptions().getDocumentCodec(), collection.getCodec(), getBufferProvider(), getSession(), false);
        int i = 0;
        while (cursor.hasNext()) {
            cursor.next();
            i++;
        }
        assertEquals(10, i);
    }

    @Test
    public void testLimitExhaustion() {
        cursor = new MongoQueryCursor<Document>(collection.getNamespace(), new Find().limit(5),
                collection.getOptions().getDocumentCodec(), collection.getCodec(), getBufferProvider(), getSession(), false);
        int i = 0;
        while (cursor.hasNext()) {
            cursor.next();
            i++;
        }
        assertEquals(5, i);
    }

    @Test
    public void testRemove() {
        cursor = new MongoQueryCursor<Document>(collection.getNamespace(), new Find().limit(2),
                collection.getOptions().getDocumentCodec(), collection.getCodec(), getBufferProvider(), getSession(), false);
        try {
            cursor.remove();
            fail();
        } catch (UnsupportedOperationException e) { // NOPMD
            // all good
        }
    }

    @Test
    public void testToString() {
        cursor = new MongoQueryCursor<Document>(collection.getNamespace(), new Find().limit(2),
                collection.getOptions().getDocumentCodec(), collection.getCodec(), getBufferProvider(), getSession(), false);
        assertTrue(cursor.toString().startsWith("MongoQueryCursor"));
    }

    @Test
    public void testSizesAndNumGetMores() {
        cursor = new MongoQueryCursor<Document>(collection.getNamespace(), new Find().batchSize(2),
                collection.getOptions().getDocumentCodec(), collection.getCodec(), getBufferProvider(), getSession(), false);
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
        database.tools().createCollection(new CreateCollectionOptions(getCollectionName(), true, 1000));

        collection.insert(new Document("_id", 1).append("ts", new BSONTimestamp(5, 0)));

        cursor = new MongoQueryCursor<Document>(collection.getNamespace(), new Find()
                .filter(new Document("ts", new Document("$gte", new BSONTimestamp(5, 0))))
                .batchSize(2)
                .addFlags(EnumSet.of(QueryFlag.Tailable, QueryFlag.AwaitData)),
                collection.getOptions().getDocumentCodec(), collection.getCodec(), getBufferProvider(), getSession(), false);
        assertTrue(cursor.hasNext());
        assertEquals(1, cursor.next().get("_id"));
        assertTrue(cursor.hasNext());

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(500);
                    collection.insert(new Document("_id", 2).append("ts", new BSONTimestamp(6, 0)));
                } catch (InterruptedException e) { // NOPMD
                    // all good
                }
            }
        }).start();

        // Note: this test is racy.  There is no guarantee that we're testing what we're trying to, which is the loop in the next() method.
        assertEquals(2, cursor.next().get("_id"));
    }

    @Test
    @Category(Slow.class)
    public void testTailableAwaitInterrupt() throws InterruptedException {
        collection.tools().drop();
        database.tools().createCollection(new CreateCollectionOptions(getCollectionName(), true, 1000));

        collection.insert(new Document("_id", 1));

        cursor = new MongoQueryCursor<Document>(collection.getNamespace(), new Find()
                .batchSize(2)
                .addFlags(EnumSet.of(QueryFlag.Tailable, QueryFlag.AwaitData)),
                collection.getOptions().getDocumentCodec(), collection.getCodec(), getBufferProvider(), getSession(), false);

        final CountDownLatch latch = new CountDownLatch(1);
        final List<Boolean> success = new ArrayList<Boolean>();
        final Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    cursor.next();
                    cursor.next();
                } catch (MongoInterruptedException e) {
                    success.add(true);
                } finally {
                    latch.countDown();
                }
            }
        });
        t.start();
        Thread.sleep(1000);  // Note: this is racy, as where the interrupted exception is actually thrown from depends on timing.
        t.interrupt();
        latch.await();
        assertFalse(success.isEmpty());
    }

    @Test(expected = MongoCursorNotFoundException.class)
    //@Ignore("Won't work with replica sets or when tests are run in parallel")
    public void shouldKillCursorIfLimitIsReachedOnInitialQuery() throws InterruptedException {
        cursor = new MongoQueryCursor<Document>(collection.getNamespace(), new Find().limit(5),
                collection.getOptions().getDocumentCodec(), collection.getCodec(), getBufferProvider(), getSession(), false);

        Thread.sleep(1000); //Note: waiting for some time for killCursor operation to be performed on a server.
        makeAdditionalGetMoreCall();
    }

    @Test(expected = MongoCursorNotFoundException.class)
    //@Ignore("Won't work with replica sets or when tests are run in parallel")
    public void shouldKillCursorIfLimitIsReachedOnGetMore() throws InterruptedException {
        cursor = new MongoQueryCursor<Document>(collection.getNamespace(), new Find().batchSize(3).limit(5),
                collection.getOptions().getDocumentCodec(), collection.getCodec(), getBufferProvider(), getSession(), false);

        cursor.next();
        cursor.next();
        cursor.next();
        cursor.next();

        Thread.sleep(1000); //Note: waiting for some time for killCursor operation to be performed on a server.
        makeAdditionalGetMoreCall();
    }

    @Test
    public void testLimitWithGetMore() {
        final List<Document> list = new ArrayList<Document>();
        collection.find().withQueryOptions(new QueryOptions().batchSize(2)).limit(5).into(list);
        assertEquals(5, list.size());
    }

    @Test
    public void testLimitWithLargeDocuments() {
        final char[] array = new char[16000];
        Arrays.fill(array, 'x');
        final String bigString = new String(array);

        for (int i = 11; i < 1000; i++) {
            collection.insert(new Document("_id", i).append("s", bigString));
        }

        final List<Document> list = new ArrayList<Document>();
        collection.find().limit(300).into(list);
        assertEquals(300, list.size());
    }

    @Test
    public void testNormalLoopWithGetMore() {
        cursor = new MongoQueryCursor<Document>(collection.getNamespace(), new Find()
                .batchSize(2).order(new Document("_id", 1)),
                collection.getOptions().getDocumentCodec(), collection.getCodec(), getBufferProvider(), getSession(), false);
        try {
            int i = 0;
            while (cursor.hasNext()) {
                final Document cur = cursor.next();
                assertEquals(i++, cur.get("_id"));
            }
            assertEquals(10, i);
            assertFalse(cursor.hasNext());
        } finally {
            cursor.close();
        }
    }

    @Test(expected = NoSuchElementException.class)
    public void testNextWithoutHasNextWithGetMore() {
        cursor = new MongoQueryCursor<Document>(collection.getNamespace(), new Find()
                .batchSize(2).order(new Document("_id", 1)),
                collection.getOptions().getDocumentCodec(), collection.getCodec(), getBufferProvider(), getSession(), false);
        for (int i = 0; i < 10; i++) {
            final Document cur = cursor.next();
            assertEquals(i, cur.get("_id"));
        }
        assertFalse(cursor.hasNext());
        assertFalse(cursor.hasNext());
        cursor.next();
    }

    @Test
    public void shouldThrowCursorNotFoundException() {
        cursor = new MongoQueryCursor<Document>(collection.getNamespace(), new Find().batchSize(2),
                collection.getOptions().getDocumentCodec(), collection.getCodec(), getBufferProvider(), getSession(), false);
        new KillCursorProtocolOperation(new KillCursor(cursor.getServerCursor()), getBufferProvider(),
                cursor.getServerConnectionProvider().getServerDescription(), cursor.getServerConnectionProvider().getConnection(),
                true).execute();
        cursor.next();
        cursor.next();
        try {
            cursor.next();
        } catch (MongoCursorNotFoundException e) {
            assertEquals(cursor.getServerCursor(), e.getCursor());
        } catch (NoSuchElementException e) {
            fail();
        }
    }


    private void makeAdditionalGetMoreCall() {
        new GetMoreOperation<Document>(collection.getNamespace(), new GetMore(cursor.getServerCursor(), 1, 1, 1),
                collection.getOptions().getDocumentCodec(), getBufferProvider(), cursor.getServerConnectionProvider()
                .getServerDescription(), cursor.getServerConnectionProvider().getConnection(), true).execute();
    }
}
