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

import category.Async;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mongodb.async.AsyncBlock;
import org.mongodb.connection.SingleResultCallback;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

@Category(Async.class)
public class MongoAsyncReadTest extends DatabaseTestCase {
    private CountDownLatch latch;
    private List<Document> documentList;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        latch = new CountDownLatch(1);
        documentList = new ArrayList<Document>();
        for (int i = 0; i < 10; i++) {
            documentList.add(new Document("_id", i));
        }

        collection.insert(documentList);
    }

    @Test
    public void testCountFuture() throws ExecutionException, InterruptedException {
        assertThat(collection.find().asyncCount().get(), is(10L));
    }

    @Test
    public void testCountCallback() throws InterruptedException {
        final List<Long> actual = new ArrayList<Long>();
        collection.find().asyncCount().register(new SingleResultCallback<Long>() {
            @Override
            public void onResult(final Long result, final MongoException e) {
                actual.add(result);
                latch.countDown();
            }
        });

        latch.await();
        assertThat(actual.get(0), is(10L));
    }

    @Test
    public void testOneFuture() throws ExecutionException, InterruptedException {
        assertNull(collection.find(new Document("_id", 11)).asyncOne().get());
        assertThat(collection.find().sort(new Document("_id", 1)).asyncOne().get(), is(documentList.get(0)));
    }

    @Test
    public void testOneCallback() throws ExecutionException, InterruptedException {
        final List<Document> documentResultList = new ArrayList<Document>();
        final List<Exception> exceptionList = new ArrayList<Exception>();
        collection.find(new Document("_id", 11)).asyncOne().register(new SingleResultCallback<Document>() {
            @Override
            public void onResult(final Document result, final MongoException e) {
                documentResultList.add(result);
                exceptionList.add(e);
                latch.countDown();
            }
        });
        latch.await();
        assertThat(documentResultList.get(0), is(nullValue()));
        assertThat(exceptionList.get(0), is(nullValue()));
    }

    @Test
    public void testForEach() throws InterruptedException {
        final List<Document> documentResultList = new ArrayList<Document>();
        collection.find().sort(new Document("_id", 1)).asyncForEach(new AsyncBlock<Document>() {
            @Override
            public void done() {
                latch.countDown();
            }

            @Override
            public boolean run(final Document document) {
                documentResultList.add(document);
                return true;
            }
        });

        latch.await();
        assertThat(documentResultList, is(documentList));
    }

    @Test
    public void testIntoFuture() throws ExecutionException, InterruptedException {
        assertThat(collection.find().sort(new Document("_id", 1))
                .asyncInto(new ArrayList<Document>())
                .get(),
                is(documentList));
    }

    @Test
    public void testMapIntoFuture() throws ExecutionException, InterruptedException {
        assertThat(collection.find().sort(new Document("_id", 1))
                .map(new Function<Document, Integer>() {
                    @Override
                    public Integer apply(final Document document) {
                        return (Integer) document.get("_id");
                    }
                })
                .asyncInto(new ArrayList<Integer>()).get(),
                is(asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)));
    }

    @Test
    public void testIntoCallback() throws ExecutionException, InterruptedException {
        List<Document> results = new ArrayList<Document>();
        collection.find().sort(new Document("_id", 1)).asyncInto(results).register(new SingleResultCallback<List<Document>>() {
            @Override
            public void onResult(final List<Document> result, final MongoException e) {
                latch.countDown();
            }
        });

        latch.await();
        assertThat(results, is(documentList));
    }
}
