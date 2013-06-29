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
import org.bson.ByteBuf;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mongodb.AsyncOperation;
import org.mongodb.AsyncServerSelectingOperation;
import org.mongodb.DatabaseTestCase;
import org.mongodb.Document;
import org.mongodb.MongoException;
import org.mongodb.ReadPreference;
import org.mongodb.connection.AsyncConnection;
import org.mongodb.connection.AsyncServerConnection;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.connection.ResponseSettings;
import org.mongodb.connection.ServerAddress;
import org.mongodb.connection.ServerDescription;
import org.mongodb.connection.SingleResultCallback;
import org.mongodb.operation.Find;
import org.mongodb.operation.MongoFuture;
import org.mongodb.operation.ReadPreferenceServerSelector;
import org.mongodb.operation.SingleResultFuture;
import org.mongodb.session.AsyncServerSelectingSession;
import org.mongodb.session.AsyncSession;
import org.mongodb.session.SessionBindingType;

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
import static org.mongodb.Fixture.getBufferProvider;
import static org.mongodb.Fixture.getCluster;
import static org.mongodb.assertions.Assertions.isTrue;
import static org.mongodb.operation.QueryFlag.Exhaust;

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
                new Find().batchSize(2), collection.getOptions().getDocumentCodec(), collection.getCodec(), getBufferProvider(),
                getAsyncSession(), new TestBlock()).start();
        latch.await();
        assertEquals(documentList, documentResultList);
    }

    @Test
    public void testLimit() throws InterruptedException {
        new MongoAsyncQueryCursor<Document>(collection.getNamespace(),
                new Find().batchSize(2).limit(100).order(new Document("_id", 1)),
                collection.getOptions().getDocumentCodec(), collection.getCodec(), getBufferProvider(), getAsyncSession(),
                new TestBlock()).start();

        latch.await();
        assertThat(documentResultList, is(documentList.subList(0, 100)));
    }

    @Test
    public void testExhaust() throws InterruptedException {
        new MongoAsyncQueryCursor<Document>(collection.getNamespace(),
                new Find().batchSize(2).addFlags(EnumSet.of(Exhaust)).order(new Document("_id", 1)),
                collection.getOptions().getDocumentCodec(), collection.getCodec(), getBufferProvider(), getAsyncSession(),
                new TestBlock()).start();

        latch.await();
        assertThat(documentResultList, is(documentList));
    }

    @Test
    public void testExhaustWithLimit() throws InterruptedException {
        new MongoAsyncQueryCursor<Document>(collection.getNamespace(),
                new Find().batchSize(2).limit(5).addFlags(EnumSet.of(Exhaust)).order(new Document("_id", 1)),
                collection.getOptions().getDocumentCodec(), collection.getCodec(), getBufferProvider(), getAsyncSession(),
                new TestBlock()).start();

        latch.await();
        assertThat(documentResultList, is(documentList.subList(0, 5)));
    }

    @Test
    public void testExhaustWithDiscard() throws InterruptedException, ExecutionException {
        AsyncServerConnection connection = getCluster().getServer(new ReadPreferenceServerSelector(ReadPreference.primary()))
                .getAsyncConnection();

        try {
            SingleConnectionAsyncServerSelectingSession session = new SingleConnectionAsyncServerSelectingSession(connection);

            new MongoAsyncQueryCursor<Document>(collection.getNamespace(),
                    new Find().batchSize(2).limit(5).addFlags(EnumSet.of(Exhaust)).order(new Document("_id", 1)),
                    collection.getOptions().getDocumentCodec(), collection.getCodec(), getBufferProvider(), session, new TestBlock(1))
                    .start();

            latch.await();
            assertThat(documentResultList, is(documentList.subList(0, 1)));

            documentResultList.clear();
            CountDownLatch nextLatch = new CountDownLatch(1);

            new MongoAsyncQueryCursor<Document>(collection.getNamespace(),
                    new Find().limit(1).order(new Document("_id", -1)),
                    collection.getOptions().getDocumentCodec(), collection.getCodec(), getBufferProvider(), session, new TestBlock(1,
                    nextLatch)).start();
            nextLatch.await();
            assertEquals(Arrays.asList(new Document("_id", 999)), documentResultList);
        } finally {
            connection.close();
        }
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

    private static final class SingleConnectionAsyncServerSelectingSession implements AsyncServerSelectingSession {
        private AsyncServerConnection connection;
        private boolean isClosed;

        public SingleConnectionAsyncServerSelectingSession(final AsyncServerConnection connection) {
            this.connection = connection;
        }

        public <T> MongoFuture<T> execute(final AsyncServerSelectingOperation<T> operation) {
            final SingleResultFuture<T> retVal = new SingleResultFuture<T>();

            operation.execute(connection).register(
                    new ConnectionClosingSingleResultCallback<T>(new DelayedCloseAsyncConnection(connection), retVal));

            return retVal;
        }

        @Override
        public <T> MongoFuture<AsyncSession> getBoundSession(final AsyncServerSelectingOperation<T> operation,
                                                             final SessionBindingType sessionBindingType) {
            return new SingleResultFuture<AsyncSession>(
                    new SingleConnectionAsyncSession(new DelayedCloseAsyncConnection(connection)), null);
        }

        @Override
        public void close() {
            isClosed = true;
        }

        @Override
        public boolean isClosed() {
            return isClosed;
        }
    }

    private static final class SingleConnectionAsyncSession implements AsyncSession {
        private AsyncServerConnection connection;
        private boolean isClosed;

        public SingleConnectionAsyncSession(final AsyncServerConnection connection) {
            this.connection = connection;
        }

        /**
         * Executes the given operation.
         *
         * @param operation the operation to execute
         * @param <T>       the return type of the operation
         * @return a future for the result of the operation
         */
        @Override
        public <T> MongoFuture<T> execute(final AsyncOperation<T> operation) {
            final SingleResultFuture<T> retVal = new SingleResultFuture<T>();

            operation.execute(connection).register(
                    new ConnectionClosingSingleResultCallback<T>(new DelayedCloseAsyncConnection(connection), retVal));

            return retVal;
        }

        @Override
        public void close() {
            isClosed = true;
        }

        @Override
        public boolean isClosed() {
            return isClosed;
        }
    }

    private static class DelayedCloseAsyncConnection implements AsyncServerConnection {
        private AsyncServerConnection wrapped;
        private boolean isClosed;

        public DelayedCloseAsyncConnection(final AsyncServerConnection asyncConnection) {
            wrapped = asyncConnection;
        }

        @Override
        public ServerAddress getServerAddress() {
            return wrapped.getServerAddress();
        }

        @Override
        public void sendMessage(final List<ByteBuf> byteBuffers, final SingleResultCallback<Void> callback) {
            isTrue("open", !isClosed());
            wrapped.sendMessage(byteBuffers, callback);
        }

        @Override
        public void receiveMessage(final ResponseSettings responseSettings, final SingleResultCallback<ResponseBuffers> callback) {
            isTrue("open", !isClosed());
            wrapped.receiveMessage(responseSettings, callback);
        }

        @Override
        public void close() {
            isClosed = true;
        }

        @Override
        public boolean isClosed() {
            return isClosed;
        }

        @Override
        public ServerDescription getDescription() {
            isTrue("open", !isClosed());
            return wrapped.getDescription();
        }
    }

    private static class ConnectionClosingSingleResultCallback<T> implements SingleResultCallback<T> {
        private final AsyncConnection connection;
        private final SingleResultFuture<T> retVal;

        public ConnectionClosingSingleResultCallback(final AsyncConnection connection, final SingleResultFuture<T> retVal) {
            this.connection = connection;
            this.retVal = retVal;
        }

        @Override
        public void onResult(final T result, final MongoException e) {
            connection.close();
            retVal.init(result, e);
        }
    }
}