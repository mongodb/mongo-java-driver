/*
 * Copyright 2008-present MongoDB, Inc.
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

package documentation;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.DatabaseTestCase;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Field;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.Variable;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.After;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.client.Fixture.getDefaultDatabaseName;
import static com.mongodb.client.Fixture.getMongoClient;
import static com.mongodb.client.model.Accumulators.sum;
import static com.mongodb.client.model.Aggregates.addFields;
import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.lookup;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Aggregates.sort;
import static com.mongodb.client.model.Aggregates.unwind;
import static com.mongodb.client.model.Filters.all;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.elemMatch;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.exists;
import static com.mongodb.client.model.Filters.expr;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Filters.lte;
import static com.mongodb.client.model.Filters.or;
import static com.mongodb.client.model.Filters.regex;
import static com.mongodb.client.model.Filters.size;
import static com.mongodb.client.model.Filters.type;
import static com.mongodb.client.model.Projections.computed;
import static com.mongodb.client.model.Projections.exclude;
import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;
import static com.mongodb.client.model.Projections.slice;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.currentDate;
import static com.mongodb.client.model.Updates.set;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

// imports required for change streams
// end required change streams imports
// imports required for filters, projections and updates
// end required filters, projections and updates imports


public final class DocumentationSamples extends DatabaseTestCase {

    private final MongoDatabase database = getMongoClient().getDatabase(getDefaultDatabaseName());
    private final MongoCollection<Document> collection = database.getCollection("inventory");

    @Test
    public void testInsert() {

        // Start Example 1
        Document canvas = new Document("item", "canvas")
                .append("qty", 100)
                .append("tags", singletonList("cotton"));

        Document size = new Document("h", 28)
                .append("w", 35.5)
                .append("uom", "cm");
        canvas.put("size", size);

        collection.insertOne(canvas);
        // End Example 1

        // Start Example 2
        FindIterable<Document> findIterable = collection.find(eq("item", "canvas"));
        // End Example 2

        assertEquals(1, findIterable.into(new ArrayList<Document>()).size());

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

        collection.insertMany(asList(journal, mat, mousePad));
        // End Example 3

        assertEquals(4, collection.countDocuments());
    }

    @Test
    public void testQueryingAtTheTopLevel() {
        // Start Example 6
        collection.insertMany(asList(
                Document.parse("{ item: 'journal', qty: 25, size: { h: 14, w: 21, uom: 'cm' }, status: 'A' }"),
                Document.parse("{ item: 'notebook', qty: 50, size: { h: 8.5, w: 11, uom: 'in' }, status: 'A' }"),
                Document.parse("{ item: 'paper', qty: 100, size: { h: 8.5, w: 11, uom: 'in' }, status: 'D' }"),
                Document.parse("{ item: 'planner', qty: 75, size: { h: 22.85, w: 30, uom: 'cm' }, status: 'D' }"),
                Document.parse("{ item: 'postcard', qty: 45, size: { h: 10, w: 15.25, uom: 'cm' }, status: 'A' }")
        ));
        // End Example 6

        assertEquals(5, collection.countDocuments());

        // Start Example 7
        FindIterable<Document> findIterable = collection.find(new Document());
        // End Example 7

        assertEquals(5, findIterable.into(new ArrayList<Document>()).size());

        // Start Example 8
        findIterable = collection.find();
        // End Example 8

        assertEquals(5, findIterable.into(new ArrayList<Document>()).size());

        // Start Example 9
        findIterable = collection.find(eq("status", "D"));
        // End Example 9

        assertEquals(2, findIterable.into(new ArrayList<Document>()).size());

        // Start Example 10
        findIterable = collection.find(in("status", "A", "D"));
        // End Example 10

        assertEquals(5, findIterable.into(new ArrayList<Document>()).size());

        // Start Example 11
        findIterable = collection.find(and(eq("status", "A"), lt("qty", 30)));
        // End Example 11

        assertEquals(1, findIterable.into(new ArrayList<Document>()).size());

        // Start Example 12
        findIterable = collection.find(or(eq("status", "A"), lt("qty", 30)));
        // End Example 12

        assertEquals(3, findIterable.into(new ArrayList<Document>()).size());

        // Start Example 13
        findIterable = collection.find(
                and(eq("status", "A"),
                        or(lt("qty", 30), regex("item", "^p")))
        );
        // End Example 13

        assertEquals(2, findIterable.into(new ArrayList<Document>()).size());
    }

    @Test
    public void testQueryingEmbeddedDocuments() {
        // Start Example 14
        collection.insertMany(asList(
                Document.parse("{ item: 'journal', qty: 25, size: { h: 14, w: 21, uom: 'cm' }, status: 'A' }"),
                Document.parse("{ item: 'notebook', qty: 50, size: { h: 8.5, w: 11, uom: 'in' }, status: 'A' }"),
                Document.parse("{ item: 'paper', qty: 100, size: { h: 8.5, w: 11, uom: 'in' }, status: 'D' }"),
                Document.parse("{ item: 'planner', qty: 75, size: { h: 22.85, w: 30, uom: 'cm' }, status: 'D' }"),
                Document.parse("{ item: 'postcard', qty: 45, size: { h: 10, w: 15.25, uom: 'cm' }, status: 'A' }")
        ));
        // End Example 14

        assertEquals(5, collection.countDocuments());

        // Start Example 15
        FindIterable<Document> findIterable = collection.find(eq("size", Document.parse("{ h: 14, w: 21, uom: 'cm' }")));
        // End Example 15

        assertEquals(1, findIterable.into(new ArrayList<Document>()).size());

        // Start Example 16
        findIterable = collection.find(eq("size", Document.parse("{ w: 21, h: 14, uom: 'cm' }")));
        // End Example 16

        assertEquals(0, findIterable.into(new ArrayList<Document>()).size());

        // Start Example 17
        findIterable = collection.find(eq("size.uom", "in"));
        // End Example 17

        assertEquals(2, findIterable.into(new ArrayList<Document>()).size());

        // Start Example 18
        findIterable = collection.find(lt("size.h", 15));
        // End Example 18

        assertEquals(4, findIterable.into(new ArrayList<Document>()).size());

        // Start Example 19
        findIterable = collection.find(and(
                lt("size.h", 15),
                eq("size.uom", "in"),
                eq("status", "D")
        ));
        // End Example 19

        assertEquals(1, findIterable.into(new ArrayList<Document>()).size());
    }

    @Test
    public void testQueryingArrayValues() {

        //Start Example 20
        collection.insertMany(asList(
                Document.parse("{ item: 'journal', qty: 25, tags: ['blank', 'red'], dim_cm: [ 14, 21 ] }"),
                Document.parse("{ item: 'notebook', qty: 50, tags: ['red', 'blank'], dim_cm: [ 14, 21 ] }"),
                Document.parse("{ item: 'paper', qty: 100, tags: ['red', 'blank', 'plain'], dim_cm: [ 14, 21 ] }"),
                Document.parse("{ item: 'planner', qty: 75, tags: ['blank', 'red'], dim_cm: [ 22.85, 30 ] }"),
                Document.parse("{ item: 'postcard', qty: 45, tags: ['blue'], dim_cm: [ 10, 15.25 ] }")
        ));
        //End Example 20

        assertEquals(5, collection.countDocuments());

        //Start Example 21
        FindIterable<Document> findIterable = collection.find(eq("tags", asList("red", "blank")));
        //End Example 21

        assertEquals(1, findIterable.into(new ArrayList<Document>()).size());

        //Start Example 22
        findIterable = collection.find(all("tags", asList("red", "blank")));
        //End Example 22

        assertEquals(4, findIterable.into(new ArrayList<Document>()).size());

        //Start Example 23
        findIterable = collection.find(eq("tags", "red"));
        //End Example 23

        assertEquals(4, findIterable.into(new ArrayList<Document>()).size());

        //Start Example 24
        findIterable = collection.find(gt("dim_cm", 25));
        //End Example 24

        assertEquals(1, findIterable.into(new ArrayList<Document>()).size());

        //Start Example 25
        findIterable = collection.find(and(gt("dim_cm", 15), lt("dim_cm", 20)));
        //End Example 25

        assertEquals(4, findIterable.into(new ArrayList<Document>()).size());

        //Start Example 26
        findIterable = collection.find(elemMatch("dim_cm", Document.parse("{ $gt: 22, $lt: 30 }")));
        //End Example 26

        assertEquals(1, findIterable.into(new ArrayList<Document>()).size());

        //Start Example 27
        findIterable = collection.find(gt("dim_cm.1", 25));
        //End Example 27

        assertEquals(1, findIterable.into(new ArrayList<Document>()).size());

        //Start Example 28
        findIterable = collection.find(size("tags", 3));
        //End Example 28

        assertEquals(1, findIterable.into(new ArrayList<Document>()).size());
    }

    @Test
    public void testQueryingArraysContainingDocuments() {

        //Start Example 29
        collection.insertMany(asList(
                Document.parse("{ item: 'journal', instock: [ { warehouse: 'A', qty: 5 }, { warehouse: 'C', qty: 15 } ] }"),
                Document.parse("{ item: 'notebook', instock: [ { warehouse: 'C', qty: 5 } ] }"),
                Document.parse("{ item: 'paper', instock: [ { warehouse: 'A', qty: 60 }, { warehouse: 'B', qty: 15 } ] }"),
                Document.parse("{ item: 'planner', instock: [ { warehouse: 'A', qty: 40 }, { warehouse: 'B', qty: 5 } ] }"),
                Document.parse("{ item: 'postcard', instock: [ { warehouse: 'B', qty: 15 }, { warehouse: 'C', qty: 35 } ] }")
        ));
        //End Example 29

        assertEquals(5, collection.countDocuments());

        //Start Example 30
        FindIterable<Document> findIterable = collection.find(eq("instock", Document.parse("{ warehouse: 'A', qty: 5 }")));
        //End Example 30

        assertEquals(1, findIterable.into(new ArrayList<Document>()).size());

        //Start Example 31
        findIterable = collection.find(eq("instock", Document.parse("{ qty: 5, warehouse: 'A' }")));
        //End Example 31

        assertEquals(0, findIterable.into(new ArrayList<Document>()).size());

        //Start Example 32
        findIterable = collection.find(lte("instock.0.qty", 20));
        //End Example 32

        assertEquals(3, findIterable.into(new ArrayList<Document>()).size());

        //Start Example 33
        findIterable = collection.find(lte("instock.qty", 20));
        //End Example 33

        assertEquals(5, findIterable.into(new ArrayList<Document>()).size());

        //Start Example 34
        findIterable = collection.find(elemMatch("instock", Document.parse("{ qty: 5, warehouse: 'A' }")));
        //End Example 34

        assertEquals(1, findIterable.into(new ArrayList<Document>()).size());

        //Start Example 35
        findIterable = collection.find(elemMatch("instock", Document.parse("{ qty: { $gt: 10, $lte: 20 } }")));
        //End Example 35

        assertEquals(3, findIterable.into(new ArrayList<Document>()).size());

        //Start Example 36
        findIterable = collection.find(and(gt("instock.qty", 10), lte("instock.qty", 20)));
        //End Example 36

        assertEquals(4, findIterable.into(new ArrayList<Document>()).size());

        //Start Example 37
        findIterable = collection.find(and(eq("instock.qty", 5), eq("instock.warehouse", "A")));
        //End Example 37

        assertEquals(2, findIterable.into(new ArrayList<Document>()).size());
    }

    @Test
    public void testQueryingNullandMissingFields() {

        //Start Example 38
        collection.insertMany(asList(
                Document.parse("{'_id': 1, 'item': null}"),
                Document.parse("{'_id': 2}")
        ));
        //End Example 38

        assertEquals(2, collection.countDocuments());

        //Start Example 39
        FindIterable<Document> findIterable = collection.find(eq("item", null));
        //End Example 39

        assertEquals(2, findIterable.into(new ArrayList<Document>()).size());

        //Start Example 40
        findIterable = collection.find(type("item", BsonType.NULL));
        //End Example 40

        assertEquals(1, findIterable.into(new ArrayList<Document>()).size());

        //Start Example 41
        findIterable = collection.find(exists("item", false));
        //End Example 41

        assertEquals(1, findIterable.into(new ArrayList<Document>()).size());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testProjectingFields() {

        //Start Example 42
        collection.insertMany(asList(
            Document.parse("{ item: 'journal', status: 'A', size: { h: 14, w: 21, uom: 'cm' }, instock: [ { warehouse: 'A', qty: 5 }]}"),
            Document.parse("{ item: 'notebook', status: 'A',  size: { h: 8.5, w: 11, uom: 'in' }, instock: [ { warehouse: 'C', qty: 5}]}"),
            Document.parse("{ item: 'paper', status: 'D', size: { h: 8.5, w: 11, uom: 'in' }, instock: [ { warehouse: 'A', qty: 60 }]}"),
            Document.parse("{ item: 'planner', status: 'D', size: { h: 22.85, w: 30, uom: 'cm' }, instock: [ { warehouse: 'A', qty: 40}]}"),
            Document.parse("{ item: 'postcard', status: 'A', size: { h: 10, w: 15.25, uom: 'cm' }, "
                    + "instock: [ { warehouse: 'B', qty: 15 }, { warehouse: 'C', qty: 35 } ] }")
        ));
        //End Example 42

        assertEquals(5, collection.countDocuments());

        //Start Example 43
        FindIterable<Document> findIterable = collection.find(eq("status", "A"));
        //End Example 43

        assertEquals(3, findIterable.into(new ArrayList<Document>()).size());

        //Start Example 44
        findIterable = collection.find(eq("status", "A")).projection(include("item", "status"));
        //End Example 44

        findIterable.forEach(new Consumer<Document>() {
            @Override
            public void accept(final Document document) {
                assertEquals(new HashSet<String>(asList("_id", "item", "status")), document.keySet());
            }
        });

        //Start Example 45
        findIterable = collection.find(eq("status", "A"))
                .projection(fields(include("item", "status"), excludeId()));
        //End Example 45

        findIterable.forEach(new Consumer<Document>() {
            @Override
            public void accept(final Document document) {
                assertEquals(new HashSet<String>(asList("item", "status")), document.keySet());
            }
        });

        //Start Example 46
        findIterable = collection.find(eq("status", "A")).projection(exclude("item", "status"));
        //End Example 46

        findIterable.forEach(new Consumer<Document>() {
            @Override
            public void accept(final Document document) {
                assertEquals(new HashSet<String>(asList("_id", "size", "instock")), document.keySet());
            }
        });

        //Start Example 47
        findIterable = collection.find(eq("status", "A")).projection(include("item", "status", "size.uom"));
        //End Example 47

        findIterable.forEach(new Consumer<Document>() {
            @Override
            public void accept(final Document document) {
                assertEquals(new HashSet<String>(asList("_id", "item", "status", "size")), document.keySet());
                assertEquals(new HashSet<String>(singletonList("uom")), document.get("size", Document.class).keySet());
            }
        });

        //Start Example 48
        findIterable = collection.find(eq("status", "A")).projection(exclude("size.uom"));
        //End Example 48

        findIterable.forEach(new Consumer<Document>() {
            @Override
            public void accept(final Document document) {
                assertEquals(new HashSet<String>(asList("_id", "item", "instock", "status", "size")), document.keySet());
                assertEquals(new HashSet<String>(asList("h", "w")), document.get("size", Document.class).keySet());
            }
        });

        //Start Example 49
        findIterable = collection.find(eq("status", "A")).projection(include("item", "status", "instock.qty"));
        //End Example 49

        findIterable.forEach(new Consumer<Document>() {
            @Override
            public void accept(final Document document) {
                assertEquals(new HashSet<String>(asList("_id", "item", "instock", "status")), document.keySet());

                List<Document> instock = (List<Document>) document.get("instock");
                for (Document stockDocument : instock) {
                    assertEquals(new HashSet<String>(singletonList("qty")), stockDocument.keySet());
                }
            }
        });

        //Start Example 50
        findIterable = collection.find(eq("status", "A"))
                .projection(fields(include("item", "status"), slice("instock", -1)));
        //End Example 50

        findIterable.forEach(new Consumer<Document>() {
            @Override
            public void accept(final Document document) {
                assertEquals(new HashSet<String>(asList("_id", "item", "instock", "status")), document.keySet());

                List<Document> instock = (List<Document>) document.get("instock");
                assertEquals(1, instock.size());
            }
        });
    }

    @Test
    public void testAggregate() {

        assumeTrue(serverVersionAtLeast(3, 6));

        MongoCollection<Document> salesCollection = database.getCollection("sales");

        // Start Aggregation Example 1
        AggregateIterable<Document> aggregateIterable = salesCollection.aggregate(asList(
                match(eq("items.fruit", "banana")),
                sort(ascending("date"))
        ));
        // End Aggregation Example 1

        aggregateIterable.into(new ArrayList<Document>());

        // Start Aggregation Example 2
        aggregateIterable = salesCollection.aggregate(asList(
                unwind("$items"),
                match(eq("items.fruit", "banana")),
                group(new Document("day", new Document("$dayOfWeek", "$date")),
                        sum("count", "$items.quantity")),
                project(fields(
                        computed("dayOfWeek", "$_id.day"),
                        computed("numberSold", "$count"),
                        excludeId())),
                sort(Indexes.ascending("numberSold"))));
        // End Aggregation Example 2

        aggregateIterable.into(new ArrayList<Document>());

        // Start Aggregation Example 3
        aggregateIterable = salesCollection.aggregate(asList(
                unwind("$items"),
                group(new Document("day", new Document("$dayOfWeek", "$date")),
                        sum("items_old", "$items.quantity"),
                        sum("revenue", new Document("$multiply", asList("$items.quantity", "$items.price")))),
                project(fields(
                        computed("day", "$_id.day"),
                        include("revenue", "items_sold"),
                        computed("discount",
                                new Document("$cond",
                                        new Document("if", new Document("$lte", Arrays.<Object>asList("$revenue", 250)))
                                                .append("then", 25)
                                                .append("else", 0)))))));
        // End Aggregation Example 3

        aggregateIterable.into(new ArrayList<Document>());

        MongoCollection<Document> airAlliancesCollection = database.getCollection("air_alliances");

        // Start Aggregation Example 4
        aggregateIterable = airAlliancesCollection.aggregate(asList(
                lookup("air_airlines",
                        singletonList(new Variable<String>("constituents", "$airlines")),
                        singletonList(match(expr(new Document("$in", asList("$name", "$$constituents"))))),
                        "airlines"),
                project(fields(
                        excludeId(),
                        include("name"),
                        computed("airlines",
                                new Document("$filter",
                                        new Document("input", "$airlines")
                                                .append("as", "airline")
                                                .append("cond", new Document("$eq", asList("$$airline.country", "Canada")))))))));

        // End Aggregation Example 4

        aggregateIterable.into(new ArrayList<Document>());
    }

    @Test
    public void testUpdates() {
        //Start Example 51
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
        ));
        //End Example 51

        assertEquals(10, collection.countDocuments());

        //Start Example 52
        collection.updateOne(eq("item", "paper"),
                combine(set("size.uom", "cm"), set("status", "P"), currentDate("lastModified")));
        //End Example 52

        collection.find(eq("item", "paper")).forEach(new Consumer<Document>() {
            @Override
            public void accept(final Document document) {
                assertEquals("cm", document.get("size", Document.class).getString("uom"));
                assertEquals("P", document.getString("status"));
                assertTrue(document.containsKey("lastModified"));
            }
        });


        //Start Example 53
        collection.updateMany(lt("qty", 50),
                combine(set("size.uom", "in"), set("status", "P"), currentDate("lastModified")));
        //End Example 53

        collection.find(lt("qty", 50)).forEach(new Consumer<Document>() {
            @Override
            public void accept(final Document document) {
                assertEquals("in", document.get("size", Document.class).getString("uom"));
                assertEquals("P", document.getString("status"));
                assertTrue(document.containsKey("lastModified"));
            }
        });

        //Start Example 54
        collection.replaceOne(eq("item", "paper"),
                Document.parse("{ item: 'paper', instock: [ { warehouse: 'A', qty: 60 }, { warehouse: 'B', qty: 40 } ] }"));
        //End Example 54

        collection.find(eq("item", "paper")).projection(excludeId()).forEach(new Consumer<Document>() {
            @Override
            public void accept(final Document document) {
                assertEquals(Document.parse("{ item: 'paper', instock: [ { warehouse: 'A', qty: 60 }, { warehouse: 'B', qty: 40 } ] }"),
                        document);
            }
        });

    }

    @Test
    public void testDeletions() {

        //Start Example 55
        collection.insertMany(asList(
                Document.parse("{ item: 'journal', qty: 25, size: { h: 14, w: 21, uom: 'cm' }, status: 'A' }"),
                Document.parse("{ item: 'notebook', qty: 50, size: { h: 8.5, w: 11, uom: 'in' }, status: 'A' }"),
                Document.parse("{ item: 'paper', qty: 100, size: { h: 8.5, w: 11, uom: 'in' }, status: 'D' }"),
                Document.parse("{ item: 'planner', qty: 75, size: { h: 22.85, w: 30, uom: 'cm' }, status: 'D' }"),
                Document.parse("{ item: 'postcard', qty: 45, size: { h: 10, w: 15.25, uom: 'cm' }, status: 'A' }")
        ));
        //End Example 55

        assertEquals(5, collection.countDocuments());

        //Start Example 57
        collection.deleteMany(eq("status", "A"));
        //End Example 57

        assertEquals(2, collection.countDocuments());

        //Start Example 58
        collection.deleteOne(eq("status", "D"));
        //End Example 58

        assertEquals(1, collection.countDocuments());

        //Start Example 56
        collection.deleteMany(new Document());
        //End Example 56

        assertEquals(0, collection.countDocuments());
    }

    @Test
    public void testWatch() throws InterruptedException {
        assumeTrue(isDiscoverableReplicaSet() && serverVersionAtLeast(3, 6));

        final MongoCollection<Document> inventory = collection;
        final AtomicBoolean stop = new AtomicBoolean(false);

        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (!stop.get()) {
                    collection.insertMany(asList(new Document("username", "alice"), new Document()));
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                    collection.deleteOne(new Document("username", "alice"));
                }
            }
        });
        thread.start();

        // Start Changestream Example 1
        MongoCursor<ChangeStreamDocument<Document>> cursor = inventory.watch().iterator();
        ChangeStreamDocument<Document> next = cursor.next();
        // End Changestream Example 1

        cursor.close();

        // Start Changestream Example 2
        cursor = inventory.watch().fullDocument(FullDocument.UPDATE_LOOKUP).iterator();
        next = cursor.next();
        // End Changestream Example 2

        cursor.close();

        // Start Changestream Example 3
        BsonDocument resumeToken = next.getResumeToken();
        cursor = inventory.watch().resumeAfter(resumeToken).iterator();
        next = cursor.next();
        // End Changestream Example 3

        cursor.close();

        // Start Changestream Example 4
        List<Bson> pipeline = asList(match(Document.parse("{'fullDocument.username': 'alice'}")),
                addFields(new Field<String>("newField", "this is an added field!")));
        cursor = inventory.watch(pipeline).iterator();
        next = cursor.next();
        // End Changestream Example 4

        cursor.close();

        stop.set(true);
        thread.join();
    }

    @Test
    public void testRunCommand() {
        // Start runCommand Example 1
        database.runCommand(new Document("buildInfo", 1));
        // End runCommand Example 1

        database.getCollection("restaurants").drop();
        database.createCollection("restaurants");

        // Start runCommand Example 2
        database.runCommand(new Document("collStats", "restaurants"));
        // End runCommand Example 2
    }

    @Test
    public void testCreateIndexes() {

        assumeTrue(serverVersionAtLeast(3, 2));

        // Start Index Example 1
        collection.createIndex(Indexes.ascending("score"));
        // End Index Example 1

        // Start Index Example 2
        collection.createIndex(Indexes.ascending("cuisine", "name"),
                new IndexOptions().partialFilterExpression(Filters.gt("rating", 5)));
        // End Index Example 2
    }

    @After
    public void tearDown() {
        collection.drop();
    }
}
