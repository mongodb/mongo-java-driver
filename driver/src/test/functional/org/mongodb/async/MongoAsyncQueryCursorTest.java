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

package org.mongodb.async;

import category.Async;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mongodb.DatabaseTestCase;
import org.mongodb.Document;
import org.mongodb.connection.SingleConnectionAsyncSession;
import org.mongodb.operation.MongoFind;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mongodb.Fixture.getAsyncSession;
import static org.mongodb.Fixture.getBufferPool;
import static org.mongodb.Fixture.getCluster;
import static org.mongodb.Fixture.getExecutor;
import static org.mongodb.operation.QueryOption.Exhaust;

@Category(Async.class)
public class MongoAsyncQueryCursorTest extends DatabaseTestCase {

    private CountDownLatch latch;
    private List<Document> documentList;
    private List<Document> documentResultList;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        latch = new CountDownLatch(1);
        documentResultList = new ArrayList<Document>();

        documentList = new ArrayList<Document>();
        for (int i = 0; i < 1000; i++) {
            documentList.add(new Document("_id", i));
        }

        collection.insert(documentList);
    }

    @After
    public void tearDown() {
        super.tearDown();
    }

    @Test
    public void testBlockRun() throws InterruptedException {
        new MongoAsyncQueryCursor<Document>(collection.getNamespace(),
                new MongoFind().batchSize(2), collection.getOptions().getDocumentCodec(), collection.getCodec(), getBufferPool(),
                getAsyncSession(), new TestBlock(), getExecutor()).start();
        latch.await();
        assertEquals(documentList, documentResultList);
    }

    @Test
    public void testLimit() throws InterruptedException {
        new MongoAsyncQueryCursor<Document>(collection.getNamespace(),
                new MongoFind().batchSize(2).limit(100).order(new Document("_id", 1)),
                collection.getOptions().getDocumentCodec(), collection.getCodec(), getBufferPool(), getAsyncSession(),
                new TestBlock(), getExecutor()).start();

        latch.await();
        assertThat(documentResultList, is(documentList.subList(0, 100)));
    }

    @Test
    public void testExhaust() throws InterruptedException {
        new MongoAsyncQueryCursor<Document>(collection.getNamespace(),
                new MongoFind().batchSize(2).addOptions(EnumSet.of(Exhaust)).order(new Document("_id", 1)),
                collection.getOptions().getDocumentCodec(), collection.getCodec(), getBufferPool(), getAsyncSession(),
                new TestBlock(), getExecutor()).start();

        latch.await();
        assertThat(documentResultList, is(documentList));
    }

    @Test
    public void testExhaustWithLimit() throws InterruptedException {
        new MongoAsyncQueryCursor<Document>(collection.getNamespace(),
                new MongoFind().batchSize(2).limit(5).addOptions(EnumSet.of(Exhaust)).order(new Document("_id", 1)),
                collection.getOptions().getDocumentCodec(), collection.getCodec(), getBufferPool(), getAsyncSession(),
                new TestBlock(), getExecutor()).start();

        latch.await();
        assertThat(documentResultList, is(documentList.subList(0, 5)));
    }

    @Test
    public void testExhaustWithDiscard() throws InterruptedException, ExecutionException {
        SingleConnectionAsyncSession session = new SingleConnectionAsyncSession(getAsyncSession().getConnection().get(), getCluster(),
                getExecutor());

        new MongoAsyncQueryCursor<Document>(collection.getNamespace(),
                new MongoFind().batchSize(2).limit(5).addOptions(EnumSet.of(Exhaust)).order(new Document("_id", 1)),
                collection.getOptions().getDocumentCodec(), collection.getCodec(), getBufferPool(), session, new TestBlock(1),
                getExecutor()).start();

        latch.await();
        assertThat(documentResultList, is(documentList.subList(0, 1)));

        documentResultList.clear();
        CountDownLatch nextLatch = new CountDownLatch(1);

        new MongoAsyncQueryCursor<Document>(collection.getNamespace(),
                new MongoFind().limit(1).order(new Document("_id", -1)),
                collection.getOptions().getDocumentCodec(), collection.getCodec(), getBufferPool(), session, new TestBlock(1,
                nextLatch), getExecutor()).start();
        nextLatch.await();
        assertEquals(Arrays.asList(new Document("_id", 999)), documentResultList);
    }


    private final class TestBlock implements AsyncBlock<Document> {
        private int count;
        private int iterations;
        private CountDownLatch latch;

        private TestBlock() {
            this(Integer.MAX_VALUE);
        }

        private TestBlock(final int count) {
            this(count, MongoAsyncQueryCursorTest.this.latch);
        }

        private TestBlock(final int count, final CountDownLatch latch) {
            this.count = count;
            this.latch = latch;
        }

        @Override
        public void done() {
            latch.countDown();
        }

        @Override
        public boolean run(final Document document) {
            iterations++;
            documentResultList.add(document);
            return iterations < count;
        }
    }
}
