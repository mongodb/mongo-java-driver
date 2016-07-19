/*
 * Copyright 2016 MongoDB, Inc.
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

package tour;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.gridfs.*;
import com.mongodb.client.gridfs.model.*;

import com.mongodb.client.model.geojson.*;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.ValidationOptions;

import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Filters;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.*;

import static com.mongodb.client.model.Updates.*;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.*;

import java.util.ArrayList;
import java.util.List;

import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.TextSearchOptions;
import com.mongodb.client.model.IndexOptions;

import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.Arrays;
import java.io.*;
import java.nio.file.Files;



public class Examples {

    public static void main(final String[] args) {
        // To run an example, uncomment the method call.

//        runQueryExamples();
//        runWriteExamples();
//        runDBandCollectionsExamples();
//        runIndexingExamples();
//        runAggregationExamples();
//        runTextSearchExamples();
//        runGeoExamples();
//        runCommandsExample();

    }

    public static void runQueryExamples() {

       Block<Document> printBlock = new Block<Document>() {
           @Override
           public void apply(final Document document) {
               System.out.println(document.toJson());
           }
       };

       MongoClient mongoClient = new MongoClient();
       MongoDatabase database = mongoClient.getDatabase("test");
       MongoCollection<Document> collection = database.getCollection("restaurants");

       // Retrieve all documents in the collection

       collection.find().forEach(printBlock);

       // Alternate way to retrieve all documents in the collection

       collection.find(new Document()).forEach(printBlock);

       // Find documents that match the name equals "456 Cookies Shop" condition
       collection.find(eq("name", "456 Cookies Shop"))
               .forEach(printBlock);

       // Create a Document object to specify a filter condition

       collection.find(
               new Document("stars", new Document("$gte", 2)
                       .append("$lt", 5))
                       .append("categories", "Bakery")).forEach(printBlock);



       collection.find(and(gte("stars", 2), lt("stars", 5), eq("categories", "Bakery"))).forEach(printBlock);


       collection.find(and(gte("stars", 2), lt("stars", 5), eq("categories", "Bakery")))
               .projection(new Document("name", 1)
                       .append("stars", 1)
                       .append("categories",1)
                       .append("_id", 0))
               .forEach(printBlock);


       collection.find(and(gte("stars", 2), lt("stars", 5), eq("categories", "Bakery")))
           .projection(fields(include("name", "stars", "categories"), excludeId()))
           .forEach(printBlock);


       collection.find(and(gte("stars", 2), lt("stars", 5), eq("categories", "Bakery")))
           .sort(Sorts.ascending("name"))
           .forEach(printBlock);

       mongoClient.close();
    }

    public static void runWriteExamples() {

        MongoClient mongoClient = new MongoClient();

        MongoDatabase database = mongoClient.getDatabase("test");

        MongoCollection<Document> collection = database.getCollection("restaurants");

        Document document = new Document("name", "Café Con Leche")
                .append("contact", new Document("phone", "228-555-0149")
                        .append("email", "cafeconleche@example.com")
                        .append("location", Arrays.asList(-73.92502, 40.8279556)))
                .append("stars", 3)
                .append("categories", Arrays.asList("Bakery", "Coffee", "Pastries"));

        collection.insertOne(document);

        Document doc1 = new Document("name", "Amarcord Pizzeria")
                .append("contact", new Document("phone", "264-555-0193")
                        .append("email", "amarcord.pizzeria@example.net")
                        .append("location", Arrays.asList(-73.88502, 40.749556)))
                .append("stars", 2)
                .append("categories", Arrays.asList("Pizzeria", "Italian", "Pasta"));


        Document doc2 = new Document("name", "Blue Coffee Bar")
                .append("contact", new Document("phone", "604-555-0102")
                        .append("email", "bluecoffeebar@example.com")
                        .append("location", Arrays.asList(-73.97902, 40.8479556)))
                .append("stars", 5)
                .append("categories", Arrays.asList("Coffee", "Pastries"));

        List<Document> documents = new ArrayList<Document>();
        documents.add(doc1);
        documents.add(doc2);
        collection.insertMany(documents);


        UpdateResult results = collection.updateOne(
                eq("_id", new ObjectId("57506d62f57802807471dd41")),
                combine(set("stars", 1), set("contact.phone", "228-555-9999"), currentDate("lastModified")));

        System.out.println("Number of documents modified: " + results.getModifiedCount());

        results = collection.updateMany(
                eq("stars", 2),
                combine(set("stars", 0), currentDate("lastModified")));

        System.out.println("Number of documents modified: " + results.getModifiedCount());

        results = collection.updateOne(
                eq("_id", 1),
                combine(set("name", "Fresh Breads and Tulips"), currentDate("lastModified")),
                new UpdateOptions().upsert(true).bypassDocumentValidation(true));

        System.out.println("Upsert a document (via updateOne) with _id: " + results.getUpsertedId());

        results = collection.replaceOne(
                eq("_id", new ObjectId("57506d62f57802807471dd41")),
                new Document("name", "Green Salads Buffet")
                        .append("contact", "TBD")
                        .append("stars", 4)
                        .append("categories", Arrays.asList("Salads", "Health Foods", "Buffet")));

        System.out.println("Number of documents replaced: " + results.getModifiedCount());

        results = collection.replaceOne(
                eq("name", "Orange Patisserie and Gelateria"),
                new Document("stars", 5)
                        .append("contact", "TBD")
                        .append("categories", Arrays.asList("Cafe", "Pastries", "Ice Cream")),
                new UpdateOptions().upsert(true).bypassDocumentValidation(true));

        System.out.println("Upsert a document (via replaceOne) with _id: " + results.getUpsertedId());

        DeleteResult delResults = collection.deleteOne(eq("_id", new ObjectId("57506d62f57802807471dd41")));
        System.out.println("Number of documents modified: " + delResults.getDeletedCount());

        delResults = collection.deleteMany(eq("stars", 4));
        System.out.println("Number of documents modified: " + delResults.getDeletedCount());

        mongoClient.close();
    }

    public static void runAggregationExamples() {

        MongoClient mongoClient = new MongoClient();
        MongoDatabase database = mongoClient.getDatabase("test");
        MongoCollection<Document> collection = database.getCollection("restaurants");

        Block<Document> printBlock = new Block<Document>() {
            public void apply(final Document document) {
                System.out.println(document.toJson());
            }
        };

        collection.aggregate(
                Arrays.asList(
                        Aggregates.match(Filters.eq("categories", "Bakery")),
                        Aggregates.group("$stars", Accumulators.sum("count", 1)))
        ).forEach(printBlock);


        collection.aggregate(
                Arrays.asList(
                        Aggregates.project(
                                Projections.fields(
                                        Projections.excludeId(),
                                        Projections.include("name"),
                                        Projections.computed("firstCategory",
                                                Document.parse("{ $arrayElemAt: [ '$categories', 0 ] }")
                                        )
                                )
                        )
                )
        ).forEach(printBlock);

        mongoClient.close();
    }

    public static void runIndexingExamples() {

       // create ascending index on name field
        collection.createIndex(Indexes.ascending("name"));

        // create compound ascending index on stars and name fields
        collection.createIndex(Indexes.ascending("stars", "name"));

        // create descending indx on stars field
        collection.createIndex(Indexes.descending("stars"));

        // create compound descending index on stars and name fields
        collection.createIndex(Indexes.descending("stars", "name"));

        // create compound index on stars (descending) and name (ascending)

        collection.createIndex(Indexes.compoundIndex(Indexes.descending("stars"), Indexes.ascending("name")));

        // create text index on name
        collection.createIndex(Indexes.text("name"));

        // create hashed index on name
        collection.createIndex(Indexes.hashed("_id"));

        // create 2dsphere index on "contact.location" field
        collection.createIndex(Indexes.geo2dsphere("contact.location"));

        // create 2d index on "contact.location" field
        collection.createIndex(Indexes.geo2d("contact.location"));

        // create geoHaystack index
        IndexOptions haystackOption = new IndexOptions().bucketSize(1.0);
        collection.createIndex(
                Indexes.geoHaystack("contact.location", Indexes.ascending("stars")),
                haystackOption);

        // create unique index
        IndexOptions indexOptions = new IndexOptions().unique(true);
        collection.createIndex(Indexes.ascending("name", "stars"), indexOptions);

        // create partial index
        IndexOptions partialFilterIndexOptions = new IndexOptions()
                .partialFilterExpression(Filters.exists("contact.email"));
        collection.createIndex(
                Indexes.descending("name", "stars"), partialFilterIndexOptions);

        for (Document index : collection.listIndexes()) {
            System.out.println(index.toJson());
        }

        mongoClient.close();

    }

    public static void runTextSearchExamples() {

       Block<Document> printBlock = new Block<Document>() {
            @Override
            public void apply(final Document document) {
                System.out.println(document.toJson());
            }
        };

        MongoClient mongoClient = new MongoClient();
        MongoDatabase database = mongoClient.getDatabase("test");
        MongoCollection<Document> collection = database.getCollection("restaurants");

        collection.createIndex(Indexes.text("name"));

        long matchCount = collection.count(Filters.text("bakery cafe"));
        System.out.println("Text search matches: " + matchCount);


        collection.find(Filters.text("bakery cafe"))
                .projection(Projections.metaTextScore("score"))
                .sort(Sorts.metaTextScore("score")).forEach(printBlock);

        long matchCountEnglish = collection.count(Filters.text("cafe", new TextSearchOptions().language("english")));
        System.out.println("Text search matches (english): " + matchCountEnglish);

        mongoClient.close();
    }

    public static void runGeoExamples() {
       Block<Document> printBlock = new Block<Document>() {
           @Override
           public void apply(final Document document) {
               System.out.println(document.toJson());
           }
       };

       MongoClient mongoClient = new MongoClient();
       MongoDatabase database = mongoClient.getDatabase("test");
       MongoCollection<Document> collection = database.getCollection("restaurants");

       String geo2dsphereIndexName = collection.createIndex(Indexes.geo2dsphere("contact.location"));
       System.out.println("Created index: " + geo2dsphereIndexName);

       Point refPoint = new Point(new Position(-73.9667, 40.78));
       collection.find(Filters.near("contact.location", refPoint, 5000.0, 1000.0)).forEach(printBlock);

       mongoClient.close();
    }

    public static void runDBandCollectionsExamples() {
        MongoClient mongoClient = new MongoClient();
        MongoDatabase database = mongoClient.getDatabase("test");

        MongoCollection<Document> coll = database.getCollection("myTestCollection");

        database.createCollection("cappedCollection",
                new CreateCollectionOptions().capped(true).sizeInBytes(0x100000));

        ValidationOptions collOptions = new ValidationOptions().validator(
                Filters.or(Filters.exists("email"), Filters.exists("phone")));
        database.createCollection("contacts",
                new CreateCollectionOptions().validationOptions(collOptions));

        for (String name : database.listCollectionNames()) {
            System.out.println(name);
        }

        MongoCollection<Document> collection = database.getCollection("contacts");
        collection.drop();

        mongoClient.close();

    }

    public static void runCommandsExample() {

        MongoClient mongoClient = new MongoClient();
        MongoDatabase database = mongoClient.getDatabase("test");

        Document buildInfoResults = database.runCommand(new Document("buildInfo", 1));
        System.out.println(buildInfoResults.toJson());

        Document collStatsResults = database.runCommand(new Document("collStats", "restaurants"));
        System.out.println(collStatsResults.toJson());

        mongoClient.close();
    }

}


