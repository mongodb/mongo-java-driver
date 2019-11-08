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
 *
 */

package reactivestreams.documentation;

import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.reactivestreams.client.FindPublisher;
import com.mongodb.reactivestreams.client.MongoCollection;
import org.bson.BsonType;
import org.bson.Document;
import org.junit.Before;
import org.junit.Test;
import reactivestreams.helpers.SubscriberHelpers.ObservableSubscriber;
import reactivestreams.helpers.SubscriberHelpers.OperationSubscriber;

// imports required for filters, projections and updates
import static com.mongodb.client.Fixture.getDefaultDatabaseName;
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

import static com.mongodb.reactivestreams.client.Fixture.getMongoClient;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;


public final class DocumentationSamples {

    private final MongoCollection<Document> collection =
            getMongoClient().getDatabase(getDefaultDatabaseName()).getCollection("inventory");

    @Before
    public void setup() {
        ObservableSubscriber<Void> dropSubscriber = new OperationSubscriber<>();
        collection.drop().subscribe(dropSubscriber);
        dropSubscriber.await();
    }

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

        ObservableSubscriber<Void> insertSubscriber = new OperationSubscriber<>();
        collection.insertOne(canvas)
                .subscribe(insertSubscriber);
        insertSubscriber.await();
        // End Example 1

        // Start Example 2
        FindPublisher<Document> findPublisher = collection.find(eq("item", "canvas"));
        // End Example 2

        ObservableSubscriber<Document> findSubscriber = new OperationSubscriber<>();
        findPublisher.subscribe(findSubscriber);
        findSubscriber.await();

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

        insertSubscriber = new OperationSubscriber<>();
        collection.insertMany(asList(journal, mat, mousePad))
                .subscribe(insertSubscriber);
        insertSubscriber.await();
        // End Example 3

        ObservableSubscriber<Long> countSubscriber = new OperationSubscriber<>();
        collection.countDocuments().subscribe(countSubscriber);
        countSubscriber.await();
    }

    @Test
    public void testQueryingAtTheTopLevel() {
        // Start Example 6
        ObservableSubscriber<Void> insertSubscriber = new OperationSubscriber<>();
        collection.insertMany(asList(
                Document.parse("{ item: 'journal', qty: 25, size: { h: 14, w: 21, uom: 'cm' }, status: 'A' }"),
                Document.parse("{ item: 'notebook', qty: 50, size: { h: 8.5, w: 11, uom: 'in' }, status: 'A' }"),
                Document.parse("{ item: 'paper', qty: 100, size: { h: 8.5, w: 11, uom: 'in' }, status: 'D' }"),
                Document.parse("{ item: 'planner', qty: 75, size: { h: 22.85, w: 30, uom: 'cm' }, status: 'D' }"),
                Document.parse("{ item: 'postcard', qty: 45, size: { h: 10, w: 15.25, uom: 'cm' }, status: 'A' }"))
        ).subscribe(insertSubscriber);
        insertSubscriber.await();
        // End Example 6

        ObservableSubscriber<Long> countSubscriber = new OperationSubscriber<>();
        collection.countDocuments()
                .subscribe(countSubscriber);
        countSubscriber.await();

        // Start Example 7
        FindPublisher<Document> findPublisher = collection.find(new Document());
        // End Example 7

        ObservableSubscriber<Document> findSubscriber = new OperationSubscriber<>();
        findPublisher.subscribe(findSubscriber);
        findSubscriber.await();

        // Start Example 8
        findPublisher = collection.find();
        // End Example 8

        findSubscriber = new OperationSubscriber<>();
        findPublisher.subscribe(findSubscriber);
        findSubscriber.await();

        // Start Example 9
        findPublisher = collection.find(eq("status", "D"));
        // End Example 9

        findSubscriber = new OperationSubscriber<>();
        findPublisher.subscribe(findSubscriber);
        findSubscriber.await();

        // Start Example 10
        findPublisher = collection.find(in("status", "A", "D"));
        // End Example 10

        findSubscriber = new OperationSubscriber<>();
        findPublisher.subscribe(findSubscriber);
        findSubscriber.await();

        // Start Example 11
        findPublisher = collection.find(and(eq("status", "A"), lt("qty", 30)));
        // End Example 11

        findSubscriber = new OperationSubscriber<>();
        findPublisher.subscribe(findSubscriber);
        findSubscriber.await();


        // Start Example 12
        findPublisher = collection.find(or(eq("status", "A"), lt("qty", 30)));
        // End Example 12

        findSubscriber = new OperationSubscriber<>();
        findPublisher.subscribe(findSubscriber);
        findSubscriber.await();

        // Start Example 13
        findPublisher = collection.find(
                and(eq("status", "A"),
                        or(lt("qty", 30), regex("item", "^p")))
        );
        // End Example 13

        findSubscriber = new OperationSubscriber<>();
        findPublisher.subscribe(findSubscriber);
        findSubscriber.await();
    }

    @Test
    public void testQueryingEmbeddedDocuments() {
        // Start Example 14
        ObservableSubscriber<Void> insertSubscriber = new OperationSubscriber<>();
        collection.insertMany(asList(
                Document.parse("{ item: 'journal', qty: 25, size: { h: 14, w: 21, uom: 'cm' }, status: 'A' }"),
                Document.parse("{ item: 'notebook', qty: 50, size: { h: 8.5, w: 11, uom: 'in' }, status: 'A' }"),
                Document.parse("{ item: 'paper', qty: 100, size: { h: 8.5, w: 11, uom: 'in' }, status: 'D' }"),
                Document.parse("{ item: 'planner', qty: 75, size: { h: 22.85, w: 30, uom: 'cm' }, status: 'D' }"),
                Document.parse("{ item: 'postcard', qty: 45, size: { h: 10, w: 15.25, uom: 'cm' }, status: 'A' }"))
        ).subscribe(insertSubscriber);
        insertSubscriber.await();
        // End Example 14

        ObservableSubscriber<Long> countSubscriber = new OperationSubscriber<>();
        collection.countDocuments().subscribe(countSubscriber);
        countSubscriber.await();

        // Start Example 15
        FindPublisher<Document> findPublisher = collection.find(eq("size", Document.parse("{ h: 14, w: 21, uom: 'cm' }")));
        // End Example 15

        ObservableSubscriber<Document> findSubscriber = new OperationSubscriber<>();
        findPublisher.subscribe(findSubscriber);
        findSubscriber.await();

        // Start Example 16
        findPublisher = collection.find(eq("size", Document.parse("{ w: 21, h: 14, uom: 'cm' }")));
        // End Example 16

        findSubscriber = new OperationSubscriber<>();
        findPublisher.subscribe(findSubscriber);
        findSubscriber.await();

        // Start Example 17
        findPublisher = collection.find(eq("size.uom", "in"));
        // End Example 17

        findSubscriber = new OperationSubscriber<>();
        findPublisher.subscribe(findSubscriber);
        findSubscriber.await();

        // Start Example 18
        findPublisher = collection.find(lt("size.h", 15));
        // End Example 18

        findSubscriber = new OperationSubscriber<>();
        findPublisher.subscribe(findSubscriber);
        findSubscriber.await();

        // Start Example 19
        findPublisher = collection.find(and(
                lt("size.h", 15),
                eq("size.uom", "in"),
                eq("status", "D")
        ));
        // End Example 19

        findSubscriber = new OperationSubscriber<>();
        findPublisher.subscribe(findSubscriber);
        findSubscriber.await();
    }

    @Test
    public void testQueryingArrayValues() {

        //Start Example 20
        ObservableSubscriber<Void> insertSubscriber = new OperationSubscriber<>();
       collection.insertMany(asList(
                Document.parse("{ item: 'journal', qty: 25, tags: ['blank', 'red'], dim_cm: [ 14, 21 ] }"),
                Document.parse("{ item: 'notebook', qty: 50, tags: ['red', 'blank'], dim_cm: [ 14, 21 ] }"),
                Document.parse("{ item: 'paper', qty: 100, tags: ['red', 'blank', 'plain'], dim_cm: [ 14, 21 ] }"),
                Document.parse("{ item: 'planner', qty: 75, tags: ['blank', 'red'], dim_cm: [ 22.85, 30 ] }"),
                Document.parse("{ item: 'postcard', qty: 45, tags: ['blue'], dim_cm: [ 10, 15.25 ] }"))
        ).subscribe(insertSubscriber);
        insertSubscriber.await();
        // End Example 20

        ObservableSubscriber<Long> countSubscriber = new OperationSubscriber<>();
        collection.countDocuments().subscribe(countSubscriber);
        countSubscriber.await();

        //Start Example 21
        FindPublisher<Document> findPublisher = collection.find(eq("tags", asList("red", "blank")));
        //End Example 21

        ObservableSubscriber<Object> findSubscriber = new OperationSubscriber<>();
        findPublisher.subscribe(findSubscriber);
        findSubscriber.await();

        //Start Example 22
        findPublisher = collection.find(all("tags", asList("red", "blank")));
        //End Example 22

        findSubscriber = new OperationSubscriber<>();
        findPublisher.subscribe(findSubscriber);
        findSubscriber.await();

        //Start Example 23
        findPublisher = collection.find(eq("tags", "red"));
        //End Example 23

        findSubscriber = new OperationSubscriber<>();
        findPublisher.subscribe(findSubscriber);
        findSubscriber.await();

        //Start Example 24
        findPublisher = collection.find(gt("dim_cm", 25));
        //End Example 24

        findSubscriber = new OperationSubscriber<>();
        findPublisher.subscribe(findSubscriber);
        findSubscriber.await();

        //Start Example 25
        findPublisher = collection.find(and(gt("dim_cm", 15), lt("dim_cm", 20)));
        //End Example 25

        findSubscriber = new OperationSubscriber<>();
        findPublisher.subscribe(findSubscriber);
        findSubscriber.await();

        //Start Example 26
        findPublisher = collection.find(elemMatch("dim_cm", Document.parse("{ $gt: 22, $lt: 30 }")));
        //End Example 26

        findSubscriber = new OperationSubscriber<>();
        findPublisher.subscribe(findSubscriber);
        findSubscriber.await();

        //Start Example 27
        findPublisher = collection.find(gt("dim_cm.1", 25));
        //End Example 27

        findSubscriber = new OperationSubscriber<>();
        findPublisher.subscribe(findSubscriber);
        findSubscriber.await();

        //Start Example 28
        findPublisher = collection.find(size("tags", 3));
        //End Example 28

        findSubscriber = new OperationSubscriber<>();
        findPublisher.subscribe(findSubscriber);
        findSubscriber.await();
    }

    @Test
    public void testQueryingArraysContainingDocuments() {

        //Start Example 29
        ObservableSubscriber<Void> insertSubscriber = new OperationSubscriber<>();
        collection.insertMany(asList(
                Document.parse("{ item: 'journal', instock: [ { warehouse: 'A', qty: 5 }, { warehouse: 'C', qty: 15 } ] }"),
                Document.parse("{ item: 'notebook', instock: [ { warehouse: 'C', qty: 5 } ] }"),
                Document.parse("{ item: 'paper', instock: [ { warehouse: 'A', qty: 60 }, { warehouse: 'B', qty: 15 } ] }"),
                Document.parse("{ item: 'planner', instock: [ { warehouse: 'A', qty: 40 }, { warehouse: 'B', qty: 5 } ] }"),
                Document.parse("{ item: 'postcard', instock: [ { warehouse: 'B', qty: 15 }, { warehouse: 'C', qty: 35 } ] }"))
        ).subscribe(insertSubscriber);
        insertSubscriber.await();
        // End Example 29

        ObservableSubscriber<Long> countSubscriber = new OperationSubscriber<>();
        collection.countDocuments().subscribe(countSubscriber);
        countSubscriber.await();

        //Start Example 30
        FindPublisher<Document> findPublisher = collection.find(eq("instock", Document.parse("{ warehouse: 'A', qty: 5 }")));
        //End Example 30

        ObservableSubscriber<Document> findSubscriber = new OperationSubscriber<>();
        findPublisher.subscribe(findSubscriber);
        findSubscriber.await();

        //Start Example 31
        findPublisher = collection.find(eq("instock", Document.parse("{ qty: 5, warehouse: 'A' }")));
        //End Example 31

        findSubscriber = new OperationSubscriber<>();
        findPublisher.subscribe(findSubscriber);
        findSubscriber.await();

        //Start Example 32
        findPublisher = collection.find(lte("instock.0.qty", 20));
        //End Example 32

        findSubscriber = new OperationSubscriber<>();
        findPublisher.subscribe(findSubscriber);
        findSubscriber.await();

        //Start Example 33
        findPublisher = collection.find(lte("instock.qty", 20));
        //End Example 33

        findSubscriber = new OperationSubscriber<>();
        findPublisher.subscribe(findSubscriber);
        findSubscriber.await();

        //Start Example 34
        findPublisher = collection.find(elemMatch("instock", Document.parse("{ qty: 5, warehouse: 'A' }")));
        //End Example 34

        findSubscriber = new OperationSubscriber<>();
        findPublisher.subscribe(findSubscriber);
        findSubscriber.await();

        //Start Example 35
        findPublisher = collection.find(elemMatch("instock", Document.parse("{ qty: { $gt: 10, $lte: 20 } }")));
        //End Example 35

        findSubscriber = new OperationSubscriber<>();
        findPublisher.subscribe(findSubscriber);
        findSubscriber.await();

        //Start Example 36
        findPublisher = collection.find(and(gt("instock.qty", 10), lte("instock.qty", 20)));
        //End Example 36

        findSubscriber = new OperationSubscriber<>();
        findPublisher.subscribe(findSubscriber);
        findSubscriber.await();
        //Start Example 37
        findPublisher = collection.find(and(eq("instock.qty", 5), eq("instock.warehouse", "A")));
        //End Example 37

        findSubscriber = new OperationSubscriber<>();
        findPublisher.subscribe(findSubscriber);
        findSubscriber.await();
    }

    @Test
    public void testQueryingNullandMissingFields() {

        //Start Example 38
        ObservableSubscriber<Void> insertSubscriber = new OperationSubscriber<>();
        collection.insertMany(asList(
                Document.parse("{'_id': 1, 'item': null}"),
                Document.parse("{'_id': 2}"))
        ).subscribe(insertSubscriber);
        insertSubscriber.await();
        // End Example 38

        ObservableSubscriber<Long> countSubscriber = new OperationSubscriber<>();
        collection.countDocuments().subscribe(countSubscriber);
        countSubscriber.await();

        //Start Example 39
        FindPublisher<Document> findPublisher = collection.find(eq("item", null));
        //End Example 39

        ObservableSubscriber<Document> findSubscriber = new OperationSubscriber<>();
        findPublisher.subscribe(findSubscriber);
        findSubscriber.await();

        //Start Example 40
        findPublisher = collection.find(type("item", BsonType.NULL));
        //End Example 40

        findSubscriber = new OperationSubscriber<>();
        findPublisher.subscribe(findSubscriber);
        findSubscriber.await();

        //Start Example 41
        findPublisher = collection.find(exists("item", false));
        //End Example 41

        findSubscriber = new OperationSubscriber<>();
        findPublisher.subscribe(findSubscriber);
        findSubscriber.await();
    }

    @Test
    public void testProjectingFields() {

        //Start Example 42
        ObservableSubscriber<Void> insertSubscriber = new OperationSubscriber<>();
        collection.insertMany(asList(
            Document.parse("{ item: 'journal', status: 'A', size: { h: 14, w: 21, uom: 'cm' }, instock: [ { warehouse: 'A', qty: 5 }]}"),
            Document.parse("{ item: 'notebook', status: 'A',  size: { h: 8.5, w: 11, uom: 'in' }, instock: [ { warehouse: 'C', qty: 5}]}"),
            Document.parse("{ item: 'paper', status: 'D', size: { h: 8.5, w: 11, uom: 'in' }, instock: [ { warehouse: 'A', qty: 60 }]}"),
            Document.parse("{ item: 'planner', status: 'D', size: { h: 22.85, w: 30, uom: 'cm' }, instock: [ { warehouse: 'A', qty: 40}]}"),
            Document.parse("{ item: 'postcard', status: 'A', size: { h: 10, w: 15.25, uom: 'cm' }, "
                    + "instock: [ { warehouse: 'B', qty: 15 }, { warehouse: 'C', qty: 35 } ] }"))
        ).subscribe(insertSubscriber);
        insertSubscriber.await();
        // End Example 42

        ObservableSubscriber<Long> countSubscriber = new OperationSubscriber<>();
        collection.countDocuments().subscribe(countSubscriber);
        countSubscriber.await();

        //Start Example 43
        FindPublisher<Document> findPublisher = collection.find(eq("status", "A"));
        //End Example 43

        ObservableSubscriber<Document> findSubscriber = new OperationSubscriber<>();
        findPublisher.subscribe(findSubscriber);
        findSubscriber.await();

        //Start Example 44
        findPublisher = collection.find(eq("status", "A")).projection(include("item", "status"));
        //End Example 44

        findSubscriber = new OperationSubscriber<>();
        findPublisher.subscribe(findSubscriber);
        findSubscriber.await();

        //Start Example 45
        findPublisher = collection.find(eq("status", "A"))
                .projection(fields(include("item", "status"), excludeId()));
        //End Example 45

        findSubscriber = new OperationSubscriber<>();
        findPublisher.subscribe(findSubscriber);
        findSubscriber.await();

        //Start Example 46
        findPublisher = collection.find(eq("status", "A")).projection(exclude("item", "status"));
        //End Example 46

        findSubscriber = new OperationSubscriber<>();
        findPublisher.subscribe(findSubscriber);
        findSubscriber.await();

        //Start Example 47
        findPublisher = collection.find(eq("status", "A")).projection(include("item", "status", "size.uom"));
        //End Example 47

        findSubscriber = new OperationSubscriber<>();
        findPublisher.subscribe(findSubscriber);
        findSubscriber.await();

        //Start Example 48
        findPublisher = collection.find(eq("status", "A")).projection(exclude("size.uom"));
        //End Example 48

        findSubscriber = new OperationSubscriber<>();
        findPublisher.subscribe(findSubscriber);
        findSubscriber.await();

        //Start Example 49
        findPublisher = collection.find(eq("status", "A")).projection(include("item", "status", "instock.qty"));
        //End Example 49

        findSubscriber = new OperationSubscriber<>();
        findPublisher.subscribe(findSubscriber);
        findSubscriber.await();

        //Start Example 50
        findPublisher = collection.find(eq("status", "A"))
                .projection(fields(include("item", "status"), slice("instock", -1)));
        //End Example 50

        findSubscriber = new OperationSubscriber<>();
        findPublisher.subscribe(findSubscriber);
        findSubscriber.await();
    }

    @Test
    public void testUpdates() {
        //Start Example 51
        ObservableSubscriber<Void> insertSubscriber = new OperationSubscriber<>();
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
                Document.parse("{ item: 'sketch pad', qty: 95, size: { h: 22.85, w: 30.5, uom: 'cm' }, status: 'A' }"))
        ).subscribe(insertSubscriber);
        insertSubscriber.await();
        // End Example 51

        ObservableSubscriber<Long> countSubscriber = new OperationSubscriber<>();
        collection.countDocuments().subscribe(countSubscriber);
        countSubscriber.await();

        //Start Example 52
        ObservableSubscriber<UpdateResult> updateSubscriber = new OperationSubscriber<>();
        collection.updateOne(eq("item", "paper"),
                combine(set("size.uom", "cm"), set("status", "P"), currentDate("lastModified")))
                .subscribe(updateSubscriber);
        updateSubscriber.await();
        //End Example 52

        FindPublisher<Document> findPublisher = collection.find(eq("item", "paper"));

        ObservableSubscriber<Document> findSubscriber = new OperationSubscriber<>();
        findPublisher.subscribe(findSubscriber);
        findSubscriber.await();

        //Start Example 53
        updateSubscriber = new OperationSubscriber<>();
        collection.updateMany(lt("qty", 50),
                combine(set("size.uom", "in"), set("status", "P"), currentDate("lastModified")))
                .subscribe(updateSubscriber);
        updateSubscriber.await();
        //End Example 53

        findPublisher = collection.find(lt("qty", 50));

        findSubscriber = new OperationSubscriber<>();
        findPublisher.subscribe(findSubscriber);
        findSubscriber.await();

        //Start Example 54
        updateSubscriber = new OperationSubscriber<>();
        collection.replaceOne(eq("item", "paper"),
                Document.parse("{ item: 'paper', instock: [ { warehouse: 'A', qty: 60 }, { warehouse: 'B', qty: 40 } ] }"))
                .subscribe(updateSubscriber);
        updateSubscriber.await();
        //End Example 54

        findPublisher = collection.find(eq("item", "paper")).projection(excludeId());

        findSubscriber = new OperationSubscriber<>();
        findPublisher.subscribe(findSubscriber);
        findSubscriber.await();
    }

    @Test
    public void testDeletions() {

        //Start Example 55
        ObservableSubscriber<Void> insertSubscriber = new OperationSubscriber<>();
        collection.insertMany(asList(
                Document.parse("{ item: 'journal', qty: 25, size: { h: 14, w: 21, uom: 'cm' }, status: 'A' }"),
                Document.parse("{ item: 'notebook', qty: 50, size: { h: 8.5, w: 11, uom: 'in' }, status: 'A' }"),
                Document.parse("{ item: 'paper', qty: 100, size: { h: 8.5, w: 11, uom: 'in' }, status: 'D' }"),
                Document.parse("{ item: 'planner', qty: 75, size: { h: 22.85, w: 30, uom: 'cm' }, status: 'D' }"),
                Document.parse("{ item: 'postcard', qty: 45, size: { h: 10, w: 15.25, uom: 'cm' }, status: 'A' }"))
        ).subscribe(insertSubscriber);
        insertSubscriber.await();
        // End Example 55

        ObservableSubscriber<Long> countSubscriber = new OperationSubscriber<>();
        collection.countDocuments().subscribe(countSubscriber);
        countSubscriber.await();

        //Start Example 57
        ObservableSubscriber<DeleteResult> deleteSubscriber = new OperationSubscriber<>();
        collection.deleteMany(eq("status", "A"))
                .subscribe(deleteSubscriber);
        deleteSubscriber.await();
        //End Example 57

        countSubscriber = new OperationSubscriber<>();
        collection.countDocuments().subscribe(countSubscriber);
        countSubscriber.await();

        //Start Example 58
        deleteSubscriber = new OperationSubscriber<>();
        collection.deleteOne(eq("status", "D"))
                .subscribe(deleteSubscriber);
        deleteSubscriber.await();
        //End Example 58

        countSubscriber = new OperationSubscriber<>();
        collection.countDocuments().subscribe(countSubscriber);
        countSubscriber.await();

        //Start Example 56
        deleteSubscriber = new OperationSubscriber<>();
        collection.deleteMany(new Document())
                .subscribe(deleteSubscriber);
        deleteSubscriber.await();
        //End Example 56

        countSubscriber = new OperationSubscriber<>();
        collection.countDocuments().subscribe(countSubscriber);
        countSubscriber.await();
    }

}
