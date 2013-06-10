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







package org.mongodb

import category.Slow
import org.bson.types.BSONTimestamp
import org.junit.experimental.categories.Category
import org.mongodb.operation.*
import org.mongodb.session.PrimaryServerSelector
import spock.lang.Specification

import java.util.concurrent.CountDownLatch

import static org.junit.Assert.assertEquals
import static org.junit.Assert.fail
import static org.mongodb.Fixture.getBufferProvider
import static org.mongodb.Fixture.getSession
import static org.mongodb.session.SessionBindingType.Server

class MongoQueryCursorSpecification extends Specification {
    private MongoQueryCursor<Document> cursor;

    protected static MongoDatabase database;
    protected MongoCollection<Document> collection;
    protected String collectionName;

    def setupSpec() {
        if (database == null) {
            database = Fixture.getMongoClient().getDatabase("DriverTest-" + System.nanoTime());
            Runtime.getRuntime().addShutdownHook(new DatabaseTestCase.ShutdownHook());
        }
    }

    def setup() {
        collectionName = getClass().getName();
        collection = database.getCollection(collectionName);
        collection.tools().drop();

        for (int i = 0; i < 10; i++) {
            collection.insert(new Document("_id", i));
        }
    }

    def cleanup() {
        if (cursor != null) {
            cursor.close();
        }
        if (collection != null) {
            collection.tools().drop();
        }
    }

    public void 'server cursor should not be null'() {
        when:
        cursor = new MongoQueryCursor<Document>(collection.getNamespace(),
                new Find().batchSize(2),
                collection.getOptions().getDocumentCodec(),
                collection.getCodec(),
                getSession(),
                getBufferProvider());

        then:
        cursor.getServerCursor() != null;
    }

    public void 'test server address'() {
        when:
        cursor = new MongoQueryCursor<Document>(collection.getNamespace(),
                new Find(),
                collection.getOptions().getDocumentCodec(),
                collection.getCodec(),
                getSession(),
                getBufferProvider());
        then:
        cursor.getServerCursor() == null;
        cursor.getServerAddress() != null;
    }

    public void 'should be able to retrieve the original find criteria from the cursor'() {
        when:
        final Find find = new Find().batchSize(2);
        cursor = new MongoQueryCursor<Document>(collection.getNamespace(), find,
                collection.getOptions().getDocumentCodec(), collection.getCodec(), getSession(), getBufferProvider());

        then:
        cursor.getCriteria() == find;
    }

    public void 'should get Exceptions for operations on the cursor after closing'() {
        cursor = new MongoQueryCursor<Document>(collection.getNamespace(), new Find(),
                collection.getOptions().getDocumentCodec(), collection.getCodec(), getSession(), getBufferProvider());

        when:
        cursor.close();
        cursor.close();

        and:
        cursor.next();

        then:
        thrown(IllegalStateException)

        when:
        cursor.hasNext();

        then:
        thrown(IllegalStateException)

        when:
        cursor.getServerCursor();

        then:
        thrown(IllegalStateException)
    }

    public void 'should throw an Exception when going off the end'() {
        cursor = new MongoQueryCursor<Document>(collection.getNamespace(), new Find().limit(2),
                collection.getOptions().getDocumentCodec(), collection.getCodec(), getSession(), getBufferProvider());

        when:
        cursor.next();
        cursor.next();
        cursor.next();

        then:
        thrown(NoSuchElementException);
    }

    public void 'test normal exhaustion'() {
        cursor = new MongoQueryCursor<Document>(collection.getNamespace(), new Find(),
                collection.getOptions().getDocumentCodec(), collection.getCodec(), getSession(), getBufferProvider());

        when:
        int i = 0;
        while (cursor.hasNext()) {
            cursor.next();
            i++;
        }

        then:
        i == 10;
    }

    public void 'test limit exhaustion'() {
        cursor = new MongoQueryCursor<Document>(collection.getNamespace(), new Find().limit(5),
                collection.getOptions().getDocumentCodec(), collection.getCodec(), getSession(), getBufferProvider());

        when:
        int i = 0;
        while (cursor.hasNext()) {
            cursor.next();
            i++;
        }

        then:
        i == 5;
    }

    public void 'test remove'() {
        cursor = new MongoQueryCursor<Document>(collection.getNamespace(), new Find().limit(2),
                collection.getOptions().getDocumentCodec(), collection.getCodec(), getSession(), getBufferProvider());

        when:
        cursor.remove();

        then:
        thrown(UnsupportedOperationException)
    }

    public void 'test to string'() {
        when:
        cursor = new MongoQueryCursor<Document>(collection.getNamespace(), new Find().limit(2),
                collection.getOptions().getDocumentCodec(), collection.getCodec(), getSession(), getBufferProvider());

        then:
        cursor.toString().startsWith("MongoQueryCursor");
    }

    public void 'test sizes and num get mores'() {
        when:
        cursor = new MongoQueryCursor<Document>(collection.getNamespace(), new Find().batchSize(2),
                collection.getOptions().getDocumentCodec(), collection.getCodec(), getSession(), getBufferProvider());

        then:
        cursor.getNumGetMores() == 0;
        cursor.getSizes().size() == 1;
        cursor.getSizes().get(0) == 2;

        when:
        cursor.next();
        cursor.next();
        cursor.next();

        then:
        cursor.getNumGetMores() == 1;
        cursor.getSizes().size() == 2;
        cursor.getSizes().get(1) == 2;

        when:
        cursor.next();
        cursor.next();

        then:
        cursor.getNumGetMores() == 2;
        cursor.getSizes().size() == 3;
        cursor.getSizes().get(2) == 2;
    }

    @Category(Slow.class)
    public void 'test tailable await'() {
        collection.tools().drop();
        database.tools().createCollection(new CreateCollectionOptions(collectionName, true, 1000));

        collection.insert(new Document("_id", 1).append("ts", new BSONTimestamp(5, 0)));

        when:
        cursor = new MongoQueryCursor<Document>(collection.getNamespace(), new Find()
                .filter(new Document("ts", new Document('$gte', new BSONTimestamp(5, 0))))
                .batchSize(2)
                .addOptions(EnumSet.of(QueryOption.Tailable, QueryOption.AwaitData)),
                collection.getOptions().getDocumentCodec(), collection.getCodec(), getSession(), getBufferProvider());

        then:
        cursor.hasNext();
        cursor.next().get("_id") == 1;
        cursor.hasNext();

        when:
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(500);
                    collection.insert(new Document("_id", 2).append("ts", new BSONTimestamp(6, 0)));
                } catch (InterruptedException e) {
                    // all good
                }
            }
        }).start();

        // Note: this 'test is racy.  There is no guarantee that we're testing what we're trying to, which is the loop in the next'() method.
        then:
        cursor.next().get("_id") == 2;
    }

    @Category(Slow.class)
    public void 'test tailable await interrupt'() throws InterruptedException {
        collection.tools().drop();
        database.tools().createCollection(new CreateCollectionOptions(collectionName, true, 1000));

        collection.insert(new Document(" id", 1));

        when:
        cursor = new MongoQueryCursor<Document>(collection.getNamespace(), new Find()
                .batchSize(2)
                .addOptions(EnumSet.of(QueryOption.Tailable, QueryOption.AwaitData)),
                collection.getOptions().getDocumentCodec(), collection.getCodec(), getSession(), getBufferProvider());

        final CountDownLatch latch = new CountDownLatch(1);
        final List<Boolean> success = new ArrayList<Boolean>();
        Thread t = new Thread(new Runnable() {
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

        then:
        !success.isEmpty();
    }

    public void 'should kill cursor if limit is reached on initial query'() throws InterruptedException {
        cursor = new MongoQueryCursor<Document>(collection.getNamespace(), new Find().limit(5),
                collection.getOptions().getDocumentCodec(), collection.getCodec(), getSession(), getBufferProvider());

        final ServerCursor serverCursor = cursor.getServerCursor();
        Thread.sleep(1000); //Note: waiting for some time for killCursor operation to be performed on a server.

        when:
        makeAdditionalGetMoreCall(serverCursor);

        then:
        thrown(MongoCursorNotFoundException)
    }

    public void 'should kill cursor if limit is reached on get more'() throws InterruptedException {
        cursor = new MongoQueryCursor<Document>(collection.getNamespace(), new Find().batchSize(3).limit(5),
                collection.getOptions().getDocumentCodec(), collection.getCodec(), getSession(), getBufferProvider());

        final ServerCursor serverCursor = cursor.getServerCursor();

        cursor.next();
        cursor.next();
        cursor.next();
        cursor.next();

        Thread.sleep(1000); //Note: waiting for some time for killCursor operation to be performed on a server.
        when:
        makeAdditionalGetMoreCall(serverCursor);

        then:
        thrown(MongoCursorNotFoundException)
    }

    public void 'test limit with get more'() {
        when:
        final List<Document> list = new ArrayList<Document>();
        collection.batchSize(2).limit(5).into(list);

        then:
        list.size() == 5;
    }

    public void 'test limit with large documents'() {
        char[] array = 'x' * 16000
        final String bigString = new String(array);

        for (int i = 11; i < 1000; i++) {
            collection.insert(new Document(" id", i).append("s", bigString));
        }

        when:
        final List<Document> list = new ArrayList<Document>();
        collection.limit(300).into(list);

        then:
        list.size() == 300;
    }

    public void 'test normal loop with get more'() {
        when:
        cursor = new MongoQueryCursor<Document>(collection.getNamespace(), new Find()
                .batchSize(2).order(new Document("_id", 1)),
                collection.getOptions().getDocumentCodec(), collection.getCodec(), getSession(), getBufferProvider());
        then:
        int i = 0;
        while (cursor.hasNext()) {
            final Document cur = cursor.next();
            i++;
            cur.get("_id") == i;
        }
        i == 10;
        !cursor.hasNext();
    }

    public void 'test next without has next with get more'() {
        when:
        cursor = new MongoQueryCursor<Document>(collection.getNamespace(), new Find()
                .batchSize(2).order(new Document("_id", 1)),
                collection.getOptions().getDocumentCodec(), collection.getCodec(), getSession(), getBufferProvider());

        then:
        for (int i = 0; i < 10; i++) {
            final Document cur = cursor.next();
            cur.get("_id") == i;
        }
        !cursor.hasNext();
        !cursor.hasNext();

        when:
        cursor.next();

        then:
        thrown(NoSuchElementException);
    }

    public void 'should throw cursor not found exception'() {
        when:
        cursor = new MongoQueryCursor<Document>(collection.getNamespace(), new Find().batchSize(2),
                collection.getOptions().getDocumentCodec(), collection.getCodec(), getSession(), getBufferProvider());
        getSession().getBoundSession(new PrimaryServerSelector(), Server).execute(
                new KillCursorOperation(new KillCursor(cursor.getServerCursor()), getBufferProvider()));
        cursor.next();
        cursor.next();
        then:
        try {
            cursor.next();
        } catch (MongoCursorNotFoundException e) {
            assertEquals(cursor.getServerCursor(), e.getCursor());
        } catch (NoSuchElementException e) {
            fail();
        }
    }

    private void makeAdditionalGetMoreCall(final ServerCursor serverCursor) {
        getSession().getBoundSession(new PrimaryServerSelector(), Server).execute(new GetMoreOperation<Document>(
                collection.getNamespace(), new GetMore(serverCursor, 1, 1, 1), collection.getOptions().getDocumentCodec(),
                getBufferProvider()));
    }

}
