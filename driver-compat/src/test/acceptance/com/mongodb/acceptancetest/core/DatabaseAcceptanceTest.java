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
import com.mongodb.operation.CreateCollectionOptions;
import org.bson.BsonDocument;
import org.junit.Ignore;
import org.junit.Test;
import org.mongodb.Document;

import java.util.List;

import static com.mongodb.Fixture.getMongoClient;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Documents the basic functionality available for Databases via the Java driver.
 */
public class DatabaseAcceptanceTest extends DatabaseTestCase {

    @Test
    public void shouldCreateCollection() {
        database.tools().createCollection(getCollectionName());

        List<String> collections = database.tools().getCollectionNames();
        assertThat(collections.contains(getCollectionName()), is(true));
    }

    @Test
    public void shouldCreateCappedCollection() {
        database.tools().createCollection(new CreateCollectionOptions(getCollectionName(), true, 40 * 1024));

        List<String> collections = database.tools().getCollectionNames();
        assertThat(collections.contains(getCollectionName()), is(true));

        MongoCollection<Document> collection = database.getCollection(getCollectionName());
        Document collStatsCommand = new Document("collStats", getCollectionName());
        Boolean isCapped = database.executeCommand(collStatsCommand, ReadPreference.primary())
                                   .getResponse().getBoolean("capped").getValue();
        assertThat(isCapped, is(true));

        assertThat("Should have the default index on _id", collection.tools().getIndexes().size(), is(1));
    }

    @Test
    public void shouldCreateCappedCollectionWithoutAutoIndex() {
        database.tools().createCollection(new CreateCollectionOptions(getCollectionName(), true, 40 * 1024, false));

        List<String> collections = database.tools().getCollectionNames();
        assertThat(collections.contains(getCollectionName()), is(true));

        MongoCollection<Document> collection = database.getCollection(getCollectionName());
        Document collStatsCommand = new Document("collStats", getCollectionName());
        Boolean isCapped = database.executeCommand(collStatsCommand, ReadPreference.primary())
                                   .getResponse().getBoolean("capped").getValue();
        assertThat(isCapped, is(true));

        assertThat("Should NOT have the default index on _id", collection.tools().getIndexes().size(), is(0));
    }

    @Test
    public void shouldSupportMaxNumberOfDocumentsInACappedCollection() {
        int maxDocuments = 5;
        database.tools().createCollection(new CreateCollectionOptions(getCollectionName(),
                                                                      true,
                                                                      40 * 1024,
                                                                      false,
                                                                      maxDocuments));

        List<String> collections = database.tools().getCollectionNames();
        assertThat(collections.contains(getCollectionName()), is(true));

        Document collStatsCommand = new Document("collStats", getCollectionName());
        BsonDocument collectionStatistics = database.executeCommand(collStatsCommand, ReadPreference.primary()).getResponse();
        assertThat("max is set correctly in collection statistics", collectionStatistics.getInt32("max").getValue(), is(maxDocuments));
    }

    @Test
    public void shouldGetCollectionNamesFromDatabase() {
        database.tools().createCollection(getCollectionName());

        List<String> collections = database.tools().getCollectionNames();

        assertThat(collections.contains("system.indexes"), is(true));
        assertThat(collections.contains(getCollectionName()), is(true));
    }

    @Test
    public void shouldChangeACollectionNameWhenRenameIsCalled() {
        //given
        String originalCollectionName = "originalCollection";
        MongoCollection<Document> originalCollection = database.getCollection(originalCollectionName);
        originalCollection.insert(new Document("someKey", "someValue"));

        assertThat(database.tools().getCollectionNames().contains(originalCollectionName), is(true));

        //when
        String newCollectionName = "TheNewCollectionName";
        database.tools().renameCollection(originalCollectionName, newCollectionName);

        //then
        assertThat(database.tools().getCollectionNames().contains(originalCollectionName), is(false));
        assertThat(database.tools().getCollectionNames().contains(newCollectionName), is(true));

        MongoCollection<Document> renamedCollection = database.getCollection(newCollectionName);
        assertThat("Renamed collection should have the same number of documents as original",
                   renamedCollection.find().count(), is(1L));
    }

    @Test
    public void shouldBeAbleToRenameCollectionToAnExistingCollectionNameAndReplaceItWhenDropIsTrue() {
        //given
        String existingCollectionName = "anExistingCollection";
        String originalCollectionName = "someOriginalCollection";

        MongoCollection<Document> originalCollection = database.getCollection(originalCollectionName);
        String keyInOriginalCollection = "someKey";
        String valueInOriginalCollection = "someValue";
        originalCollection.insert(new Document(keyInOriginalCollection, valueInOriginalCollection));

        MongoCollection<Document> existingCollection = database.getCollection(existingCollectionName);
        String keyInExistingCollection = "aDifferentDocument";
        String valueInExistingCollection = "withADifferentValue";
        existingCollection.insert(new Document(keyInExistingCollection, valueInExistingCollection));

        assertThat(database.tools().getCollectionNames().contains(originalCollectionName), is(true));
        assertThat(database.tools().getCollectionNames().contains(existingCollectionName), is(true));

        //when
        database.tools().renameCollection(originalCollectionName, existingCollectionName, true);

        //then
        assertThat(database.tools().getCollectionNames().contains(originalCollectionName), is(false));
        assertThat(database.tools().getCollectionNames().contains(existingCollectionName), is(true));

        MongoCollection<Document> replacedCollection = database.getCollection(existingCollectionName);
        assertThat(replacedCollection.find().getOne().get(keyInExistingCollection), is(nullValue()));
        assertThat(replacedCollection.find().getOne().get(keyInOriginalCollection).toString(), is(valueInOriginalCollection));
    }

    @Test
    @Ignore("not implemented")
    public void shouldFailRenameIfSharded() {

    }

    @Test
    public void shouldBeAbleToListAllTheDatabasesAvailable() {
        MongoClient mongoClient = getMongoClient();
        MongoDatabase firstDatabase = mongoClient.getDatabase("FirstNewDatabase");
        MongoDatabase secondDatabase = mongoClient.getDatabase("SecondNewDatabase");
        MongoDatabase otherDatabase = mongoClient.getDatabase("DatabaseThatDoesNotExistYet");

        try {
            // given
            firstDatabase.getCollection("coll").insert(new Document("aDoc", "to force database creation"));
            secondDatabase.getCollection("coll").insert(new Document("aDoc", "to force database creation"));

            //when
            List<String> databaseNames = mongoClient.getDatabaseNames();

            //then
            assertThat(databaseNames, hasItems(firstDatabase.getName(), secondDatabase.getName()));
            assertThat(databaseNames, not(hasItem(otherDatabase.getName())));
        } finally {
            //tear down
            firstDatabase.tools().drop();
            secondDatabase.tools().drop();
        }
    }
}
