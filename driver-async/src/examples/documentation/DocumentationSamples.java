/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package documentation;

import com.mongodb.Block;
import com.mongodb.MongoNamespace;
import com.mongodb.async.FutureResultCallback;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.DatabaseTestCase;
import com.mongodb.async.client.FindIterable;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.BsonType;
import org.bson.Document;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.async.client.Fixture.getDefaultDatabaseName;
import static com.mongodb.async.client.Fixture.initializeCollection;

// imports required for filters, projections and updates
import static com.mongodb.client.model.Filters.all;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.elemMatch;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.exists;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Filters.lte;
import static com.mongodb.client.model.Filters.or;
import static com.mongodb.client.model.Filters.regex;
import static com.mongodb.client.model.Filters.size;
import static com.mongodb.client.model.Filters.type;
import static com.mongodb.client.model.Projections.exclude;
import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;
import static com.mongodb.client.model.Projections.slice;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.currentDate;
import static com.mongodb.client.model.Updates.set;
// end required filters, projections and updates imports

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;


public final class DocumentationSamples extends DatabaseTestCase {

    private final MongoCollection<Document> collection = initializeCollection(new MongoNamespace(getDefaultDatabaseName(), "inventory"));

    @Test
    public void testInsert() throws InterruptedException, ExecutionException, TimeoutException {

        // Start Example 1
        Document canvas = new Document("item", "canvas")
                .append("qty", 100)
                .append("tags", singletonList("cotton"));

        Document size = new Document("h", 28)
                .append("w", 35.5)
                .append("uom", "cm");
        canvas.put("size", size);

        final CountDownLatch insertLatch = new CountDownLatch(1);
        collection.insertOne(canvas, new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                // Process results
                insertLatch.countDown();
            }
        });
        insertLatch.await(10, TimeUnit.SECONDS);
        // End Example 1

        // Start Example 2
        FindIterable<Document> findIterable = collection.find(eq("item", "canvas"));
        // End Example 2

        final CountDownLatch check1 = new CountDownLatch(1);
        findIterable.into(new ArrayList<Document>(), new SingleResultCallback<List<Document>>() {
            @Override
            public void onResult(final List<Document> result, final Throwable t) {
                assertEquals(1, result.size());
                check1.countDown();
            }
        });
        assertTrue(check1.await(10, TimeUnit.SECONDS));

        // Start Example 3
        Document journal = new Document("item", "journal")
                .append("qty", 25)
                .append("tags", asList("blank", "red"));

        Document journalSize = new Document("h", 14)
                .append("w", 21)
                .append("uom", "cm");
        journal.put("size", journalSize);

        Document mat = new Document("item", "mat")
                .append("qty", 85)
                .append("tags", singletonList("gray"));

        Document matSize = new Document("h", 27.9)
                .append("w", 35.5)
                .append("uom", "cm");
        mat.put("size", matSize);

        Document mousePad = new Document("item", "mousePad")
                .append("qty", 25)
                .append("tags", asList("gel", "blue"));

        Document mousePadSize = new Document("h", 19)
                .append("w", 22.85)
                .append("uom", "cm");
        mousePad.put("size", mousePadSize);

        final CountDownLatch insertManyLatch = new CountDownLatch(1);
        collection.insertMany(asList(journal, mat, mousePad), new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                insertManyLatch.countDown();
            }
        });
        insertLatch.await(10, TimeUnit.SECONDS);
        // End Example 3

        final CountDownLatch check2 = new CountDownLatch(1);
        collection.count(new SingleResultCallback<Long>() {
            @Override
            public void onResult(final Long result, final Throwable t) {
                assertEquals(Long.valueOf(4), result);
                check2.countDown();
            }
        });

        assertTrue(check2.await(10, TimeUnit.SECONDS));
    }

    @Test
    public void testQueryingAtTheTopLevel() throws InterruptedException, TimeoutException, ExecutionException {
        // Start Example 6
        final CountDownLatch insertLatch = new CountDownLatch(1);
        collection.insertMany(asList(
                Document.parse("{ item: 'journal', qty: 25, size: { h: 14, w: 21, uom: 'cm' }, status: 'A' }"),
                Document.parse("{ item: 'notebook', qty: 50, size: { h: 8.5, w: 11, uom: 'in' }, status: 'A' }"),
                Document.parse("{ item: 'paper', qty: 100, size: { h: 8.5, w: 11, uom: 'in' }, status: 'D' }"),
                Document.parse("{ item: 'planner', qty: 75, size: { h: 22.85, w: 30, uom: 'cm' }, status: 'D' }"),
                Document.parse("{ item: 'postcard', qty: 45, size: { h: 10, w: 15.25, uom: 'cm' }, status: 'A' }")
        ), new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                // Process results
                insertLatch.countDown();
            }
        });
        insertLatch.await(10, TimeUnit.SECONDS);
        // End Example 6

        final CountDownLatch check1 = new CountDownLatch(1);
        collection.count(new SingleResultCallback<Long>() {
            @Override
            public void onResult(final Long result, final Throwable t) {
                assertEquals(Long.valueOf(5), result);
                check1.countDown();
            }
        });

        // Start Example 7
        FindIterable<Document> findIterable = collection.find(new Document());
        // End Example 7

        final CountDownLatch check2 = new CountDownLatch(1);
        findIterable.into(new ArrayList<Document>(), new SingleResultCallback<ArrayList<Document>>() {
            @Override
            public void onResult(final ArrayList<Document> result, final Throwable t) {
                assertEquals(5, result.size());
                check2.countDown();
            }
        });

        // Start Example 8
        findIterable = collection.find();
        // End Example 8

        final CountDownLatch check3 = new CountDownLatch(1);
        findIterable.into(new ArrayList<Document>(), new SingleResultCallback<ArrayList<Document>>() {
            @Override
            public void onResult(final ArrayList<Document> result, final Throwable t) {
                assertEquals(5, result.size());
                check3.countDown();
            }
        });

        // Start Example 9
        findIterable = collection.find(eq("status", "D"));
        // End Example 9

        final CountDownLatch check4 = new CountDownLatch(1);
        findIterable.into(new ArrayList<Document>(), new SingleResultCallback<ArrayList<Document>>() {
            @Override
            public void onResult(final ArrayList<Document> result, final Throwable t) {
                assertEquals(2, result.size());
                check4.countDown();
            }
        });

        // Start Example 10
        findIterable = collection.find(in("status", "A", "D"));
        // End Example 10

        final CountDownLatch check5 = new CountDownLatch(1);
        findIterable.into(new ArrayList<Document>(), new SingleResultCallback<ArrayList<Document>>() {
            @Override
            public void onResult(final ArrayList<Document> result, final Throwable t) {
                assertEquals(5, result.size());
                check5.countDown();
            }
        });

        // Start Example 11
        findIterable = collection.find(and(eq("status", "A"), lt("qty", 30)));
        // End Example 11

        final CountDownLatch check6 = new CountDownLatch(1);
        findIterable.into(new ArrayList<Document>(), new SingleResultCallback<ArrayList<Document>>() {
            @Override
            public void onResult(final ArrayList<Document> result, final Throwable t) {
                assertEquals(1, result.size());
                check6.countDown();
            }
        });

        // Start Example 12
        findIterable = collection.find(or(eq("status", "A"), lt("qty", 30)));
        // End Example 12

        final CountDownLatch check7 = new CountDownLatch(1);
        findIterable.into(new ArrayList<Document>(), new SingleResultCallback<ArrayList<Document>>() {
            @Override
            public void onResult(final ArrayList<Document> result, final Throwable t) {
                assertEquals(3, result.size());
                check7.countDown();
            }
        });

        // Start Example 13
        findIterable = collection.find(
                and(eq("status", "A"),
                        or(lt("qty", 30), regex("item", "^p")))
        );
        // End Example 13

        final CountDownLatch check8 = new CountDownLatch(1);
        findIterable.into(new ArrayList<Document>(), new SingleResultCallback<ArrayList<Document>>() {
            @Override
            public void onResult(final ArrayList<Document> result, final Throwable t) {
                assertEquals(2, result.size());
                check8.countDown();
            }
        });

        assertTrue(check1.await(10, TimeUnit.SECONDS));
        assertTrue(check2.await(10, TimeUnit.SECONDS));
        assertTrue(check3.await(10, TimeUnit.SECONDS));
        assertTrue(check4.await(10, TimeUnit.SECONDS));
        assertTrue(check5.await(10, TimeUnit.SECONDS));
        assertTrue(check6.await(10, TimeUnit.SECONDS));
        assertTrue(check7.await(10, TimeUnit.SECONDS));
        assertTrue(check8.await(10, TimeUnit.SECONDS));
    }

    @Test
    public void testQueryingEmbeddedDocuments() throws InterruptedException, ExecutionException, TimeoutException {
        // Start Example 14
        final CountDownLatch insertLatch = new CountDownLatch(1);
        collection.insertMany(asList(
                Document.parse("{ item: 'journal', qty: 25, size: { h: 14, w: 21, uom: 'cm' }, status: 'A' }"),
                Document.parse("{ item: 'notebook', qty: 50, size: { h: 8.5, w: 11, uom: 'in' }, status: 'A' }"),
                Document.parse("{ item: 'paper', qty: 100, size: { h: 8.5, w: 11, uom: 'in' }, status: 'D' }"),
                Document.parse("{ item: 'planner', qty: 75, size: { h: 22.85, w: 30, uom: 'cm' }, status: 'D' }"),
                Document.parse("{ item: 'postcard', qty: 45, size: { h: 10, w: 15.25, uom: 'cm' }, status: 'A' }")
        ), new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                // Process results
                insertLatch.countDown();
            }
        });
        insertLatch.await(10, TimeUnit.SECONDS);
        // End Example 14

        final CountDownLatch check1 = new CountDownLatch(1);
        collection.count(new SingleResultCallback<Long>() {
            @Override
            public void onResult(final Long result, final Throwable t) {
                assertEquals(Long.valueOf(5), result);
                check1.countDown();
            }
        });

        // Start Example 15
        FindIterable<Document> findIterable = collection.find(eq("size", Document.parse("{ h: 14, w: 21, uom: 'cm' }")));
        // End Example 15

        final CountDownLatch check2 = new CountDownLatch(1);
        findIterable.into(new ArrayList<Document>(), new SingleResultCallback<ArrayList<Document>>() {
            @Override
            public void onResult(final ArrayList<Document> result, final Throwable t) {
                assertEquals(1, result.size());
                check2.countDown();
            }
        });

        // Start Example 16
        findIterable = collection.find(eq("size", Document.parse("{ w: 21, h: 14, uom: 'cm' }")));
        // End Example 16

        final CountDownLatch check3 = new CountDownLatch(1);
        findIterable.into(new ArrayList<Document>(), new SingleResultCallback<ArrayList<Document>>() {
            @Override
            public void onResult(final ArrayList<Document> result, final Throwable t) {
                assertTrue(result.isEmpty());
                check3.countDown();
            }
        });

        // Start Example 17
        findIterable = collection.find(eq("size.uom", "in"));
        // End Example 17

        final CountDownLatch check4 = new CountDownLatch(1);
        findIterable.into(new ArrayList<Document>(), new SingleResultCallback<ArrayList<Document>>() {
            @Override
            public void onResult(final ArrayList<Document> result, final Throwable t) {
                assertEquals(2, result.size());
                check4.countDown();
            }
        });

        // Start Example 18
        findIterable = collection.find(lt("size.h", 15));
        // End Example 18

        final CountDownLatch check5 = new CountDownLatch(1);
        findIterable.into(new ArrayList<Document>(), new SingleResultCallback<ArrayList<Document>>() {
            @Override
            public void onResult(final ArrayList<Document> result, final Throwable t) {
                assertEquals(4, result.size());
                check5.countDown();
            }
        });

        // Start Example 19
        findIterable = collection.find(and(
                lt("size.h", 15),
                eq("size.uom", "in"),
                eq("status", "D")
        ));
        // End Example 19

        final CountDownLatch check6 = new CountDownLatch(1);
        findIterable.into(new ArrayList<Document>(), new SingleResultCallback<ArrayList<Document>>() {
            @Override
            public void onResult(final ArrayList<Document> result, final Throwable t) {
                assertEquals(1, result.size());
                check6.countDown();
            }
        });

        assertTrue(check1.await(10, TimeUnit.SECONDS));
        assertTrue(check2.await(10, TimeUnit.SECONDS));
        assertTrue(check3.await(10, TimeUnit.SECONDS));
        assertTrue(check4.await(10, TimeUnit.SECONDS));
        assertTrue(check5.await(10, TimeUnit.SECONDS));
        assertTrue(check6.await(10, TimeUnit.SECONDS));
    }

    @Test
    public void testQueryingArrayValues() throws InterruptedException, ExecutionException, TimeoutException {

        //Start Example 20
        final CountDownLatch insertLatch = new CountDownLatch(1);
        collection.insertMany(asList(
                Document.parse("{ item: 'journal', qty: 25, tags: ['blank', 'red'], dim_cm: [ 14, 21 ] }"),
                Document.parse("{ item: 'notebook', qty: 50, tags: ['red', 'blank'], dim_cm: [ 14, 21 ] }"),
                Document.parse("{ item: 'paper', qty: 100, tags: ['red', 'blank', 'plain'], dim_cm: [ 14, 21 ] }"),
                Document.parse("{ item: 'planner', qty: 75, tags: ['blank', 'red'], dim_cm: [ 22.85, 30 ] }"),
                Document.parse("{ item: 'postcard', qty: 45, tags: ['blue'], dim_cm: [ 10, 15.25 ] }")
        ), new SingleResultCallback<Void>() {
                    @Override
                    public void onResult(final Void result, final Throwable t) {
                        // Process results
                        insertLatch.countDown();
                    }
                });
        insertLatch.await(10, TimeUnit.SECONDS);
        // End Example 20

        final CountDownLatch check1 = new CountDownLatch(1);
        collection.count(new SingleResultCallback<Long>() {
            @Override
            public void onResult(final Long result, final Throwable t) {
                assertEquals(Long.valueOf(5), result);
                check1.countDown();
            }
        });

        //Start Example 21
        FindIterable<Document> findIterable = collection.find(eq("tags", asList("red", "blank")));
        //End Example 21

        findIterable.into(new ArrayList<Document>(), new SingleResultCallback<ArrayList<Document>>() {
            @Override
            public void onResult(final ArrayList<Document> result, final Throwable t) {
                assertEquals(1, result.size());
            }
        });

        //Start Example 22
        findIterable = collection.find(all("tags", asList("red", "blank")));
        //End Example 22

        findIterable.into(new ArrayList<Document>(), new SingleResultCallback<ArrayList<Document>>() {
            @Override
            public void onResult(final ArrayList<Document> result, final Throwable t) {
                assertEquals(4, result.size());
            }
        });

        //Start Example 23
        findIterable = collection.find(eq("tags", "red"));
        //End Example 23

        findIterable.into(new ArrayList<Document>(), new SingleResultCallback<ArrayList<Document>>() {
            @Override
            public void onResult(final ArrayList<Document> result, final Throwable t) {
                assertEquals(4, result.size());
            }
        });

        //Start Example 24
        findIterable = collection.find(gt("dim_cm", 25));
        //End Example 24

        findIterable.into(new ArrayList<Document>(), new SingleResultCallback<ArrayList<Document>>() {
            @Override
            public void onResult(final ArrayList<Document> result, final Throwable t) {
                assertEquals(1, result.size());
            }
        });

        //Start Example 25
        findIterable = collection.find(and(gt("dim_cm", 15), lt("dim_cm", 20)));
        //End Example 25

        findIterable.into(new ArrayList<Document>(), new SingleResultCallback<ArrayList<Document>>() {
            @Override
            public void onResult(final ArrayList<Document> result, final Throwable t) {
                assertEquals(4, result.size());
            }
        });

        //Start Example 26
        findIterable = collection.find(elemMatch("dim_cm", Document.parse("{ $gt: 22, $lt: 30 }")));
        //End Example 26

        findIterable.into(new ArrayList<Document>(), new SingleResultCallback<ArrayList<Document>>() {
            @Override
            public void onResult(final ArrayList<Document> result, final Throwable t) {
                assertEquals(1, result.size());
            }
        });

        //Start Example 27
        findIterable = collection.find(gt("dim_cm.1", 25));
        //End Example 27

        findIterable.into(new ArrayList<Document>(), new SingleResultCallback<ArrayList<Document>>() {
            @Override
            public void onResult(final ArrayList<Document> result, final Throwable t) {
                assertEquals(1, result.size());
            }
        });

        //Start Example 28
        findIterable = collection.find(size("tags", 3));
        //End Example 28

        findIterable.into(new ArrayList<Document>(), new SingleResultCallback<ArrayList<Document>>() {
            @Override
            public void onResult(final ArrayList<Document> result, final Throwable t) {
                assertEquals(1, result.size());
            }
        });
    }

    @Test
    public void testQueryingArraysContainingDocuments() throws InterruptedException, ExecutionException, TimeoutException {

        //Start Example 29
        final CountDownLatch insertLatch = new CountDownLatch(1);
        collection.insertMany(asList(
                Document.parse("{ item: 'journal', instock: [ { warehouse: 'A', qty: 5 }, { warehouse: 'C', qty: 15 } ] }"),
                Document.parse("{ item: 'notebook', instock: [ { warehouse: 'C', qty: 5 } ] }"),
                Document.parse("{ item: 'paper', instock: [ { warehouse: 'A', qty: 60 }, { warehouse: 'B', qty: 15 } ] }"),
                Document.parse("{ item: 'planner', instock: [ { warehouse: 'A', qty: 40 }, { warehouse: 'B', qty: 5 } ] }"),
                Document.parse("{ item: 'postcard', instock: [ { warehouse: 'B', qty: 15 }, { warehouse: 'C', qty: 35 } ] }")
        ), new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                // Process results
                insertLatch.countDown();
            }
        });
        insertLatch.await(10, TimeUnit.SECONDS);
        // End Example 29

        FutureResultCallback<Long> assertCountFuture = new FutureResultCallback<Long>();
        collection.count(assertCountFuture);
        assertEquals(Long.valueOf(5), assertCountFuture.get(10, TimeUnit.SECONDS));

        //Start Example 30
        FindIterable<Document> findIterable = collection.find(eq("instock", Document.parse("{ warehouse: 'A', qty: 5 }")));
        //End Example 30

        findIterable.into(new ArrayList<Document>(), new SingleResultCallback<ArrayList<Document>>() {
            @Override
            public void onResult(final ArrayList<Document> result, final Throwable t) {
                assertEquals(1, result.size());
            }
        });

        //Start Example 31
        findIterable = collection.find(eq("instock", Document.parse("{ qty: 5, warehouse: 'A' }")));
        //End Example 31

        findIterable.into(new ArrayList<Document>(), new SingleResultCallback<ArrayList<Document>>() {
            @Override
            public void onResult(final ArrayList<Document> result, final Throwable t) {
                assertTrue(result.isEmpty());
            }
        });

        //Start Example 32
        findIterable = collection.find(lte("instock.0.qty", 20));
        //End Example 32

        findIterable.into(new ArrayList<Document>(), new SingleResultCallback<ArrayList<Document>>() {
            @Override
            public void onResult(final ArrayList<Document> result, final Throwable t) {
                assertEquals(3, result.size());
            }
        });

        //Start Example 33
        findIterable = collection.find(lte("instock.qty", 20));
        //End Example 33

        findIterable.into(new ArrayList<Document>(), new SingleResultCallback<ArrayList<Document>>() {
            @Override
            public void onResult(final ArrayList<Document> result, final Throwable t) {
                assertEquals(5, result.size());
            }
        });

        //Start Example 34
        findIterable = collection.find(elemMatch("instock", Document.parse("{ qty: 5, warehouse: 'A' }")));
        //End Example 34

        findIterable.into(new ArrayList<Document>(), new SingleResultCallback<ArrayList<Document>>() {
            @Override
            public void onResult(final ArrayList<Document> result, final Throwable t) {
                assertEquals(1, result.size());
            }
        });

        //Start Example 35
        findIterable = collection.find(elemMatch("instock", Document.parse("{ qty: { $gt: 10, $lte: 20 } }")));
        //End Example 35

        findIterable.into(new ArrayList<Document>(), new SingleResultCallback<ArrayList<Document>>() {
            @Override
            public void onResult(final ArrayList<Document> result, final Throwable t) {
                assertEquals(3, result.size());
            }
        });

        //Start Example 36
        findIterable = collection.find(and(gt("instock.qty", 10), lte("instock.qty", 20)));
        //End Example 36

        findIterable.into(new ArrayList<Document>(), new SingleResultCallback<ArrayList<Document>>() {
            @Override
            public void onResult(final ArrayList<Document> result, final Throwable t) {
                assertEquals(4, result.size());
            }
        });

        //Start Example 37
        findIterable = collection.find(and(eq("instock.qty", 5), eq("instock.warehouse", "A")));
        //End Example 37

        findIterable.into(new ArrayList<Document>(), new SingleResultCallback<ArrayList<Document>>() {
            @Override
            public void onResult(final ArrayList<Document> result, final Throwable t) {
                assertEquals(2, result.size());
            }
        });
    }

    @Test
    public void testQueryingNullandMissingFields() throws InterruptedException, ExecutionException, TimeoutException {

        //Start Example 38
        final CountDownLatch insertLatch = new CountDownLatch(1);
        collection.insertMany(asList(
                Document.parse("{'_id': 1, 'item': null}"),
                Document.parse("{'_id': 2}")
        ), new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                // Process results
                insertLatch.countDown();
            }
        });
        insertLatch.await(10, TimeUnit.SECONDS);
        // End Example 38

        FutureResultCallback<Long> assertCountFuture = new FutureResultCallback<Long>();
        collection.count(assertCountFuture);
        assertEquals(Long.valueOf(2), assertCountFuture.get(10, TimeUnit.SECONDS));

        //Start Example 39
        FindIterable<Document> findIterable = collection.find(eq("item", null));
        //End Example 39

        findIterable.into(new ArrayList<Document>(), new SingleResultCallback<ArrayList<Document>>() {
            @Override
            public void onResult(final ArrayList<Document> result, final Throwable t) {
                assertEquals(2, result.size());
            }
        });

        //Start Example 40
        findIterable = collection.find(type("item", BsonType.NULL));
        //End Example 40

        findIterable.into(new ArrayList<Document>(), new SingleResultCallback<ArrayList<Document>>() {
            @Override
            public void onResult(final ArrayList<Document> result, final Throwable t) {
                assertEquals(1, result.size());
            }
        });

        //Start Example 41
        findIterable = collection.find(exists("item", false));
        //End Example 41

        findIterable.into(new ArrayList<Document>(), new SingleResultCallback<ArrayList<Document>>() {
            @Override
            public void onResult(final ArrayList<Document> result, final Throwable t) {
                assertEquals(1, result.size());
            }
        });
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testProjectingFields() throws InterruptedException, ExecutionException, TimeoutException {

        //Start Example 42
        final CountDownLatch insertLatch = new CountDownLatch(1);
        collection.insertMany(asList(
            Document.parse("{ item: 'journal', status: 'A', size: { h: 14, w: 21, uom: 'cm' }, instock: [ { warehouse: 'A', qty: 5 }]}"),
            Document.parse("{ item: 'notebook', status: 'A',  size: { h: 8.5, w: 11, uom: 'in' }, instock: [ { warehouse: 'C', qty: 5}]}"),
            Document.parse("{ item: 'paper', status: 'D', size: { h: 8.5, w: 11, uom: 'in' }, instock: [ { warehouse: 'A', qty: 60 }]}"),
            Document.parse("{ item: 'planner', status: 'D', size: { h: 22.85, w: 30, uom: 'cm' }, instock: [ { warehouse: 'A', qty: 40}]}"),
            Document.parse("{ item: 'postcard', status: 'A', size: { h: 10, w: 15.25, uom: 'cm' }, "
                    + "instock: [ { warehouse: 'B', qty: 15 }, { warehouse: 'C', qty: 35 } ] }")
        ), new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                // Process results
                insertLatch.countDown();
            }
        });
        insertLatch.await(10, TimeUnit.SECONDS);
        // End Example 42

        final CountDownLatch check1 = new CountDownLatch(1);
        collection.count(new SingleResultCallback<Long>() {
            @Override
            public void onResult(final Long result, final Throwable t) {
                assertEquals(Long.valueOf(5), result);
                check1.countDown();
            }
        });

        //Start Example 43
        FindIterable<Document> findIterable = collection.find(eq("status", "A"));
        //End Example 43

        final CountDownLatch check2 = new CountDownLatch(1);
        findIterable.into(new ArrayList<Document>(), new SingleResultCallback<ArrayList<Document>>() {
            @Override
            public void onResult(final ArrayList<Document> result, final Throwable t) {
                assertEquals(3, result.size());
                check2.countDown();
            }
        });

        //Start Example 44
        findIterable = collection.find(eq("status", "A")).projection(include("item", "status"));
        //End Example 44

        FutureResultCallback<Void> check3 = new FutureResultCallback<Void>();
        findIterable.forEach(new Block<Document>() {
            @Override
            public void apply(final Document document) {
                assertEquals(new HashSet<String>(asList("_id", "item", "status")), document.keySet());
            }
        }, check3);

        //Start Example 45
        findIterable = collection.find(eq("status", "A"))
                .projection(fields(include("item", "status"), excludeId()));
        //End Example 45

        FutureResultCallback<Void> check4 = new FutureResultCallback<Void>();
        findIterable.forEach(new Block<Document>() {
            @Override
            public void apply(final Document document) {
                assertEquals(new HashSet<String>(asList("item", "status")), document.keySet());
            }
        }, check4);

        //Start Example 46
        findIterable = collection.find(eq("status", "A")).projection(exclude("item", "status"));
        //End Example 46

        FutureResultCallback<Void> check5 = new FutureResultCallback<Void>();
        findIterable.forEach(new Block<Document>() {
            @Override
            public void apply(final Document document) {
                assertEquals(new HashSet<String>(asList("_id", "size", "instock")), document.keySet());
            }
        }, check5);

        //Start Example 47
        findIterable = collection.find(eq("status", "A")).projection(include("item", "status", "size.uom"));
        //End Example 47

        FutureResultCallback<Void> check6 = new FutureResultCallback<Void>();
        findIterable.forEach(new Block<Document>() {
            @Override
            public void apply(final Document document) {
                assertEquals(new HashSet<String>(asList("_id", "item", "status", "size")), document.keySet());
                assertEquals(new HashSet<String>(singletonList("uom")), document.get("size", Document.class).keySet());
            }
        }, check6);

        //Start Example 48
        findIterable = collection.find(eq("status", "A")).projection(exclude("size.uom"));
        //End Example 48

        FutureResultCallback<Void> check7 = new FutureResultCallback<Void>();
        findIterable.forEach(new Block<Document>() {
            @Override
            public void apply(final Document document) {
                assertEquals(new HashSet<String>(asList("_id", "item", "instock", "status", "size")), document.keySet());
                assertEquals(new HashSet<String>(asList("h", "w")), document.get("size", Document.class).keySet());
            }
        }, check7);

        //Start Example 49
        findIterable = collection.find(eq("status", "A")).projection(include("item", "status", "instock.qty"));
        //End Example 49

        FutureResultCallback<Void> check8 = new FutureResultCallback<Void>();
        findIterable.forEach(new Block<Document>() {
            @Override
            public void apply(final Document document) {
                assertEquals(new HashSet<String>(asList("_id", "item", "instock", "status")), document.keySet());

                List<Document> instock = (List<Document>) document.get("instock");
                for (Document stockDocument : instock) {
                    assertEquals(new HashSet<String>(singletonList("qty")), stockDocument.keySet());
                }
            }
        }, check8);

        //Start Example 50
        findIterable = collection.find(eq("status", "A"))
                .projection(fields(include("item", "status"), slice("instock", -1)));
        //End Example 50

        FutureResultCallback<Void> check9 = new FutureResultCallback<Void>();
        findIterable.forEach(new Block<Document>() {
            @Override
            public void apply(final Document document) {
                assertEquals(new HashSet<String>(asList("_id", "item", "instock", "status")), document.keySet());

                List<Document> instock = (List<Document>) document.get("instock");
                assertEquals(1, instock.size());
            }
        }, check9);

        assertTrue(check1.await(10, TimeUnit.SECONDS));
        assertTrue(check2.await(10, TimeUnit.SECONDS));
        check3.get(10, TimeUnit.SECONDS);
        check4.get(10, TimeUnit.SECONDS);
        check5.get(10, TimeUnit.SECONDS);
        check6.get(10, TimeUnit.SECONDS);
        check7.get(10, TimeUnit.SECONDS);
        check8.get(10, TimeUnit.SECONDS);
        check9.get(10, TimeUnit.SECONDS);
    }

    @Test
    public void testUpdates() throws InterruptedException, ExecutionException, TimeoutException {
        assumeTrue(serverVersionAtLeast(2, 6));
        //Start Example 51
        final CountDownLatch insertLatch = new CountDownLatch(1);
        collection.insertMany(asList(
                Document.parse("{ item: 'canvas', qty: 100, size: { h: 28, w: 35.5, uom: 'cm' }, status: 'A' }"),
                Document.parse("{ item: 'journal', qty: 25, size: { h: 14, w: 21, uom: 'cm' }, status: 'A' }"),
                Document.parse("{ item: 'mat', qty: 85, size: { h: 27.9, w: 35.5, uom: 'cm' }, status: 'A' }"),
                Document.parse("{ item: 'mousepad', qty: 25, size: { h: 19, w: 22.85, uom: 'cm' }, status: 'P' }"),
                Document.parse("{ item: 'notebook', qty: 50, size: { h: 8.5, w: 11, uom: 'in' }, status: 'P' }"),
                Document.parse("{ item: 'paper', qty: 100, size: { h: 8.5, w: 11, uom: 'in' }, status: 'D' }"),
                Document.parse("{ item: 'planner', qty: 75, size: { h: 22.85, w: 30, uom: 'cm' }, status: 'D' }"),
                Document.parse("{ item: 'postcard', qty: 45, size: { h: 10, w: 15.25, uom: 'cm' }, status: 'A' }"),
                Document.parse("{ item: 'sketchbook', qty: 80, size: { h: 14, w: 21, uom: 'cm' }, status: 'A' }"),
                Document.parse("{ item: 'sketch pad', qty: 95, size: { h: 22.85, w: 30.5, uom: 'cm' }, status: 'A' }")
        ), new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                // Process results
                insertLatch.countDown();
            }
        });
        insertLatch.await(10, TimeUnit.SECONDS);
        // End Example 51

        FutureResultCallback<Long> check1 = new FutureResultCallback<Long>();
        collection.count(check1);
        assertEquals(Long.valueOf(10), check1.get(10, TimeUnit.SECONDS));

        //Start Example 52
        final CountDownLatch updateOneLatch = new CountDownLatch(1);
        collection.updateOne(eq("item", "paper"),
                combine(set("size.uom", "cm"), set("status", "P"), currentDate("lastModified")),
                new SingleResultCallback<UpdateResult>() {
                    @Override
                    public void onResult(final UpdateResult result, final Throwable t) {
                        updateOneLatch.countDown();
                    }
                });
        updateOneLatch.await(10, TimeUnit.SECONDS);
        //End Example 52

        FutureResultCallback<Void> check2 = new FutureResultCallback<Void>();
        collection.find(eq("item", "paper")).forEach(new Block<Document>() {
            @Override
            public void apply(final Document document) {
                assertEquals("cm", document.get("size", Document.class).getString("uom"));
                assertEquals("P", document.getString("status"));
                assertTrue(document.containsKey("lastModified"));
            }
        }, check2);
        check2.get(10, TimeUnit.SECONDS);

        //Start Example 53
        final CountDownLatch updateManyLatch = new CountDownLatch(1);
        collection.updateMany(lt("qty", 50),
                combine(set("size.uom", "in"), set("status", "P"), currentDate("lastModified")),
                new SingleResultCallback<UpdateResult>() {
                    @Override
                    public void onResult(final UpdateResult result, final Throwable t) {
                        updateManyLatch.countDown();
                    }
                });
        updateManyLatch.await(10, TimeUnit.SECONDS);
        //End Example 53

        FutureResultCallback<Void> check3 = new FutureResultCallback<Void>();
        collection.find(lt("qty", 50)).forEach(new Block<Document>() {
            @Override
            public void apply(final Document document) {
                assertEquals("in", document.get("size", Document.class).getString("uom"));
                assertEquals("P", document.getString("status"));
                assertTrue(document.containsKey("lastModified"));
            }
        }, check3);
        check3.get(10, TimeUnit.SECONDS);

        //Start Example 54
        final CountDownLatch replaceOneLatch = new CountDownLatch(1);
        collection.replaceOne(eq("item", "paper"),
                Document.parse("{ item: 'paper', instock: [ { warehouse: 'A', qty: 60 }, { warehouse: 'B', qty: 40 } ] }"),
                new SingleResultCallback<UpdateResult>() {
                    @Override
                    public void onResult(final UpdateResult result, final Throwable t) {
                        replaceOneLatch.countDown();
                    }
                });
        replaceOneLatch.await(10, TimeUnit.SECONDS);
        //End Example 54

        FutureResultCallback<Void> check4 = new FutureResultCallback<Void>();
        collection.find(eq("item", "paper")).projection(excludeId()).forEach(new Block<Document>() {
            @Override
            public void apply(final Document document) {
                assertEquals(Document.parse("{ item: 'paper', instock: [ { warehouse: 'A', qty: 60 }, { warehouse: 'B', qty: 40 } ] }"),
                        document);
            }
        }, check4);
        check4.get(10, TimeUnit.SECONDS);
    }

    @Test
    public void testDeletions() throws InterruptedException, ExecutionException, TimeoutException {

        //Start Example 55
        final CountDownLatch insertLatch = new CountDownLatch(1);
        collection.insertMany(asList(
                Document.parse("{ item: 'journal', qty: 25, size: { h: 14, w: 21, uom: 'cm' }, status: 'A' }"),
                Document.parse("{ item: 'notebook', qty: 50, size: { h: 8.5, w: 11, uom: 'in' }, status: 'A' }"),
                Document.parse("{ item: 'paper', qty: 100, size: { h: 8.5, w: 11, uom: 'in' }, status: 'D' }"),
                Document.parse("{ item: 'planner', qty: 75, size: { h: 22.85, w: 30, uom: 'cm' }, status: 'D' }"),
                Document.parse("{ item: 'postcard', qty: 45, size: { h: 10, w: 15.25, uom: 'cm' }, status: 'A' }")
        ), new SingleResultCallback<Void>() {
                @Override
                public void onResult(final Void result, final Throwable t) {
                    // Process results
                    insertLatch.countDown();
                }
        });
        insertLatch.await(10, TimeUnit.SECONDS);
        // End Example 55

        FutureResultCallback<Long> check1 = new FutureResultCallback<Long>();
        collection.count(check1);
        assertEquals(Long.valueOf(5), check1.get(10, TimeUnit.SECONDS));

        //Start Example 57
        final CountDownLatch deleteOneLatch = new CountDownLatch(1);
        collection.deleteMany(eq("status", "A"), new SingleResultCallback<DeleteResult>() {
            @Override
            public void onResult(final DeleteResult result, final Throwable t) {
                deleteOneLatch.countDown();
            }
        });
        deleteOneLatch.await(10, TimeUnit.SECONDS);
        //End Example 57

        FutureResultCallback<Long> check2 = new FutureResultCallback<Long>();
        collection.count(check2);
        assertEquals(Long.valueOf(2), check2.get(10, TimeUnit.SECONDS));

        //Start Example 58
        final CountDownLatch deleteOneWithQueryLatch = new CountDownLatch(1);
        collection.deleteOne(eq("status", "D"), new SingleResultCallback<DeleteResult>() {
            @Override
            public void onResult(final DeleteResult result, final Throwable t) {
                deleteOneWithQueryLatch.countDown();
            }
        });
        deleteOneWithQueryLatch.await(10, TimeUnit.SECONDS);
        //End Example 58

        FutureResultCallback<Long> check3 = new FutureResultCallback<Long>();
        collection.count(check3);
        assertEquals(Long.valueOf(1), check3.get(10, TimeUnit.SECONDS));

        //Start Example 56
        final CountDownLatch deleteManyLatch = new CountDownLatch(1);
        collection.deleteMany(new Document(), new SingleResultCallback<DeleteResult>() {
            @Override
            public void onResult(final DeleteResult result, final Throwable t) {
                deleteManyLatch.countDown();
            }
        });
        deleteManyLatch.await(10, TimeUnit.SECONDS);
        //End Example 56

        FutureResultCallback<Long> check4 = new FutureResultCallback<Long>();
        collection.count(check4);
        assertEquals(Long.valueOf(0), check4.get(10, TimeUnit.SECONDS));
    }

}
