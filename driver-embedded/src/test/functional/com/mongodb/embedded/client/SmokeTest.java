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

package com.mongodb.embedded.client;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import org.bson.Document;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SmokeTest extends DatabaseTestCase {

    @Test
    public void shouldHandleCommonCRUDScenariosWithoutError() {
        MongoDatabase database = Fixture.getDefaultDatabase();
        MongoCollection<Document> collection = database.getCollection("test");
        collection.drop();


        Document document = new Document("_id", 1);
        Document updatedDocument = new Document("_id", 1).append("a", 1);

        assertEquals(0, collection.count());
        assertEquals(null, collection.find().first());

        collection.insertOne(document);
        assertEquals(1, collection.count());

        assertEquals(document, collection.find().first());

        assertTrue(collection.updateOne(document, Document.parse("{$set: {a: 1}}")).wasAcknowledged());
        assertEquals(updatedDocument, collection.find().first());

        assertEquals(updatedDocument, collection.aggregate(Collections.singletonList(Aggregates.match(new Document("a", 1)))).first());
        assertEquals(1, collection.deleteOne(new Document()).getDeletedCount());

        assertEquals(0, collection.count());

        collection.insertMany(asList(new Document("id", "a"), new Document("id", "b"), new Document("id", "c")));

        assertEquals(asList("a", "b", "c"), collection.distinct("id", String.class).into(new ArrayList<String>()));
    }

    @Test
    public void shouldHandleCommonAdministrativeScenariosWithoutError(){
        MongoDatabase database = Fixture.getDefaultDatabase();
        MongoClient mongoClient = Fixture.getMongoClient();

        database.drop();
        List<String> databaseNames = mongoClient.listDatabaseNames().into(new ArrayList<String>());


        String collectionName = "test";
        database.createCollection(collectionName);

        List<String> updatedDatabaseNames = mongoClient.listDatabaseNames().into(new ArrayList<String>());
        assertEquals(databaseNames.size() + 1, updatedDatabaseNames.size());

        List<String> collectionNames = database.listCollectionNames().into(new ArrayList<String>());
        assertThat(collectionNames, contains(collectionName));

        MongoCollection<Document> collection = database.getCollection(collectionName);
        collection.drop();

        assertThat(database.listCollectionNames().into(new ArrayList<String>()), not(contains(collectionName)));
    }
}
