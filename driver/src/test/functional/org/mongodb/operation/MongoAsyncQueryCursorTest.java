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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mongodb.AsyncBlock;
import org.mongodb.DatabaseTestCase;
import org.mongodb.Document;
import org.mongodb.binding.AsyncClusterBinding;
import org.mongodb.binding.AsyncConnectionSource;
import org.mongodb.binding.AsyncReadBinding;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.connection.Connection;
import org.mongodb.protocol.QueryProtocol;
import org.mongodb.protocol.QueryResult;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeFalse;
import static org.mongodb.Fixture.getAsyncBinding;
import static org.mongodb.Fixture.getBinding;
import static org.mongodb.Fixture.getCluster;
import static org.mongodb.Fixture.isSharded;
import static org.mongodb.ReadPreference.primary;
import static org.mongodb.operation.QueryFlag.Exhaust;

@Category(Async.class)
public class MongoAsyncQueryCursorTest extends DatabaseTestCase {

    private CountDownLatch latch;
    private List<Document> documentList;
    private List<Document> documentResultList;
    private AsyncReadBinding binding;
    private AsyncConnectionSource source;

    @Before
    public void setUp() {
        super.setUp();
        latch = new CountDownLatch(1);
        documentResultList = new ArrayList<Document>();

        documentList = new ArrayList<Document>();
        for (int i = 0; i < 1000; i++) {
            documentList.add(new Document("_id", i));
        }

        collection.insert(documentList);
        binding = new AsyncClusterBinding(getCluster(), primary(), 1, SECONDS);
        source = binding.getReadConnectionSource().get();
    }

    @After
    public void tearDown() {
        super.tearDown();
        binding.release();
    }

    @Test
    public void testBlockRun() throws InterruptedException {
        QueryResult<Document> firstBatch = executeQuery();
        new MongoAsyncQueryCursor<Document>(collection.getNamespace(),
                                            firstBatch, 0, 2, new DocumentCodec(),
                                            source)
        .start(new TestBlock());
        latch.await();
        assertEquals(documentList, documentResultList);
    }

    @Test
    public void testLimit() throws InterruptedException {
        QueryResult<Document> firstBatch = executeQuery();
        new MongoAsyncQueryCursor<Document>(collection.getNamespace(),
                                            firstBatch, 100, 0, new DocumentCodec(),
                                            source)
        .start(new TestBlock());

        latch.await();
        assertThat(documentResultList, is(documentList.subList(0, 100)));
    }

    @Test
    public void testExhaust() throws InterruptedException {
        assumeFalse(isSharded());
        Connection connection = source.getConnection().get();
        QueryResult<Document> firstBatch = executeQuery(getOrderedByIdQuery(),
                                                        2, EnumSet.of(Exhaust),
                                                        connection);
        new MongoAsyncQueryCursor<Document>(collection.getNamespace(),
                                            firstBatch, 0, 2, new DocumentCodec(),
                                            connection)
        .start(new TestBlock());

        latch.await();
        assertThat(documentResultList, is(documentList));
    }

    @Test
    public void testExhaustWithLimit() throws InterruptedException {
        assumeFalse(isSharded());
        Connection connection = source.getConnection().get();
        QueryResult<Document> firstBatch = executeQuery(getOrderedByIdQuery(), 2, EnumSet.of(Exhaust), connection);

        new MongoAsyncQueryCursor<Document>(collection.getNamespace(),
                                            firstBatch, 5, 2, new DocumentCodec(),
                                            connection)
        .start(new TestBlock());

        latch.await();
        assertThat(documentResultList, is(documentList.subList(0, 5)));
    }

    @Test
    public void testExhaustWithDiscard() throws InterruptedException, ExecutionException {
        assumeFalse(isSharded());

        Connection connection = getBinding().getReadConnectionSource().getConnection().retain();

        try {
            QueryResult<Document> firstBatch = executeQuery(getOrderedByIdQuery(), 2, EnumSet.of(Exhaust), connection);

            new MongoAsyncQueryCursor<Document>(collection.getNamespace(), firstBatch, 5, 2, new DocumentCodec(), connection)
            .start(new TestBlock(1));

            latch.await();
            assertThat(documentResultList, is(documentList.subList(0, 1)));

            firstBatch = executeQuery(getOrderedByIdQuery(), 1, EnumSet.of(Exhaust), connection);
            assertEquals(Arrays.asList(new Document("_id", 0)), firstBatch.getResults());
        } finally {
            connection.release();
        }
    }

    @Test
    public void testEarlyTermination() throws InterruptedException, ExecutionException {
        assumeFalse(isSharded());
        AsyncConnectionSource source = getAsyncBinding().getReadConnectionSource().get();

        try {
            Connection connection = source.getConnection().get();
            QueryResult<Document> firstBatch = executeQuery(getOrderedByIdQuery(), 2, EnumSet.of(Exhaust), connection);

            TestBlock block = new TestBlock(1);
            new MongoAsyncQueryCursor<Document>(collection.getNamespace(),
                                                firstBatch, 5, 2, new DocumentCodec(),
                                                connection)
            .start(block);

            latch.await();
            assertEquals(1, block.getIterations());
        } finally {
            source.release();
        }
    }

    private Document getOrderedByIdQuery() {
        return new Document("$query", new Document()).append("$orderby", new Document("_id", 1));
    }

    private QueryResult<Document> executeQuery() {
        return executeQuery(getOrderedByIdQuery(), 0, EnumSet.noneOf(QueryFlag.class));
    }

    private QueryResult<Document> executeQuery(final Document query, final int numberToReturn, final EnumSet<QueryFlag> queryFlag) {
        Connection connection = source.getConnection().get();
        try {
            return executeQuery(query, numberToReturn, queryFlag, connection);
        } finally {
            connection.release();
        }
    }

    private QueryResult<Document> executeQuery(final Document query, final int numberToReturn, final EnumSet<QueryFlag> queryFlag,
                                               final Connection connection) {
        return new QueryProtocol<Document>(collection.getNamespace(), queryFlag, 0, numberToReturn, query, null,
                                           new DocumentCodec(), new DocumentCodec())
               .execute(connection);
    }

    private final class TestBlock implements AsyncBlock<Document> {
        private final int count;
        private int iterations;
        private final CountDownLatch latch;

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
        public void apply(final Document document) {
            if (iterations >= count) {
                throw new RuntimeException("Discard the rest");
            }
            iterations++;
            documentResultList.add(document);
        }

        public int getIterations() {
            return iterations;
        }
    }
}
