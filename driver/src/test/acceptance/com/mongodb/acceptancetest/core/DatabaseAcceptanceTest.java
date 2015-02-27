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

package com.mongodb.acceptancetest.core;

import com.mongodb.MongoClient;
import com.mongodb.ReadPreference;
import com.mongodb.client.DatabaseTestCase;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import org.bson.Document;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.Fixture.getMongoClient;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Documents the basic functionality available for Databases via the Java driver.
 */
public class DatabaseAcceptanceTest extends DatabaseTestCase {

    @Test
    public void shouldCreateCollection() {
        database.createCollection(getCollectionName());

        List<String> collections = database.listCollectionNames().into(new ArrayList<String>());
        assertThat(collections.contains(getCollectionName()), is(true));
    }

    @Test
    public void shouldCreateCappedCollection() {
        database.createCollection(getCollectionName(), new CreateCollectionOptions().capped(true).sizeInBytes(40 * 1024));

        List<String> collections = database.listCollectionNames().into(new ArrayList<String>());
        assertThat(collections.contains(getCollectionName()), is(true));

        MongoCollection<Document> collection = database.getCollection(getCollectionName());
        Document collStatsCommand = new Document("collStats", getCollectionName());
        Boolean isCapped = database.runCommand(collStatsCommand, ReadPreference.primary()).getBoolean("capped");
        assertThat(isCapped, is(true));

        assertThat("Should have the default index on _id", collection.listIndexes().into(new ArrayList<Document>()).size(), is(1));
    }

    @Test
    public void shouldCreateCappedCollectionWithoutAutoIndex() {
        database.createCollection(getCollectionName(), new CreateCollectionOptions()
                .capped(true)
                .sizeInBytes(40 * 1024)
                .autoIndex(false));

        List<String> collections = database.listCollectionNames().into(new ArrayList<String>());
        assertThat(collections.contains(getCollectionName()), is(true));

        MongoCollection<Document> collection = database.getCollection(getCollectionName());
        Document collStatsCommand = new Document("collStats", getCollectionName());
        Boolean isCapped = database.runCommand(collStatsCommand, ReadPreference.primary()).getBoolean("capped");
        assertThat(isCapped, is(true));

        assertThat("Should NOT have the default index on _id", collection.listIndexes().into(new ArrayList<Document>()).size(), is(0));
    }

    @Test
    public void shouldSupportMaxNumberOfDocumentsInACappedCollection() {
        int maxDocuments = 5;
        database.createCollection(getCollectionName(), new CreateCollectionOptions()
                .capped(true)
                .sizeInBytes(40 * 1024)
                .autoIndex(false)
                .maxDocuments(maxDocuments));

        List<String> collections = database.listCollectionNames().into(new ArrayList<String>());
        assertThat(collections.contains(getCollectionName()), is(true));

        Document collStatsCommand = new Document("collStats", getCollectionName());
        Document collectionStatistics = database.runCommand(collStatsCommand, ReadPreference.primary());
        assertThat("max is set correctly in collection statistics", collectionStatistics.getInteger("max"), is(maxDocuments));
    }

    @Test
    public void shouldListCollectionNamesFromDatabase() {
        database.createCollection(getCollectionName());
        List<String> collections = database.listCollectionNames().into(new ArrayList<String>());

        assertThat(collections.contains(getCollectionName()), is(true));
    }

    @Test
    public void shouldListCollectionsFromDatabase() {
        database.drop();

        List<Document> collections = database.listCollections().into(new ArrayList<Document>());
        assertThat(collections.size(), is(0));

        for (int i = 0; i < 20; i++) {
            database.createCollection("coll" + i);
        }

        collections = database.listCollections().into(new ArrayList<Document>());
        assertThat(collections.size(), is(greaterThanOrEqualTo(20)));

        collections = database.listCollections().filter(new Document("name", "coll1")).into(new ArrayList<Document>());
        assertThat(collections.size(), is(1));

    }

    @Test
    public void shouldBeAbleToListAllTheDatabasesAvailable() {
        MongoClient mongoClient = getMongoClient();
        MongoDatabase firstDatabase = mongoClient.getDatabase("FirstNewDatabase");
        MongoDatabase secondDatabase = mongoClient.getDatabase("SecondNewDatabase");
        MongoDatabase otherDatabase = mongoClient.getDatabase("DatabaseThatDoesNotExistYet");

        try {
            // given
            firstDatabase.getCollection("coll").insertOne(new Document("aDoc", "to force database creation"));
            secondDatabase.getCollection("coll").insertOne(new Document("aDoc", "to force database creation"));

            //when
            List<String> databaseNames = mongoClient.listDatabaseNames().into(new ArrayList<String>());

            //then
            assertThat(databaseNames, hasItems(firstDatabase.getName(), secondDatabase.getName()));
            assertThat(databaseNames, not(hasItem(otherDatabase.getName())));
        } finally {
            //tear down
            firstDatabase.drop();
            secondDatabase.drop();
        }
    }
}
