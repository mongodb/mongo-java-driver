/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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

package org.mongodb.acceptancetest.crud;

import org.bson.types.Document;
import org.junit.Before;
import org.junit.Test;
import org.mongodb.CreateCollectionOptions;
import org.mongodb.MongoClient;
import org.mongodb.MongoCollection;
import org.mongodb.MongoDatabase;

import java.util.Set;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mongodb.acceptancetest.Fixture.getMongoClient;

/**
 * Documents the basic functionality available for Databases via the Java driver.
 */
public class DatabaseAcceptanceTest {
    private static final String DB_NAME = "DatabaseAcceptanceTest";
    private MongoDatabase database;

    @Before
    public void setUp() {
        final MongoClient mongoClient = getMongoClient();

        database = mongoClient.getDatabase(DB_NAME);
        database.admin().drop();
    }

    @Test
    public void shouldCreateCollection() {
        final String collectionName = "newCollectionName";
        database.admin().createCollection(collectionName);

        final Set<String> collections = database.admin().getCollectionNames();
        assertThat("The new collection should exist on the database", collections.size(), is(2));
        assertThat(collections.contains("newCollectionName"), is(true));
    }

    @Test
    public void shouldCreateCappedCollection() {
        final String collectionName = "newCollectionName";
        database.admin().createCollection(new CreateCollectionOptions(collectionName, true, 40 * 1024));

        final Set<String> collections = database.admin().getCollectionNames();
        assertThat(collections.contains("newCollectionName"), is(true));

        final MongoCollection<Document> collection = database.getCollection(collectionName);
        assertThat(collection.admin().isCapped(), is(true));

        assertThat("Should have the default index on _id", collection.admin().getIndexes().size(), is(1));
    }

    @Test
    public void shouldCreateCappedCollectionWithoutAutoIndex() {
        final String collectionName = "newCollectionName";
        database.admin().createCollection(new CreateCollectionOptions(collectionName, true, 40 * 1024, false));

        final Set<String> collections = database.admin().getCollectionNames();
        assertThat(collections.contains("newCollectionName"), is(true));

        final MongoCollection<Document> collection = database.getCollection(collectionName);
        assertThat(collection.admin().isCapped(), is(true));

        assertThat("Should NOT have the default index on _id", collection.admin().getIndexes().size(), is(0));
    }

    @Test
    public void shouldSupportMaxNumberOfDocumentsInACappedCollection() {
        final int maxDocuments = 5;
        final String collectionName = "newCollectionName";
        database.admin()
                .createCollection(new CreateCollectionOptions(collectionName, true, 40 * 1024, false, maxDocuments));

        final Set<String> collections = database.admin().getCollectionNames();
        assertThat(collections.contains("newCollectionName"), is(true));

        final MongoCollection<Document> collection = database.getCollection(collectionName);
        final Document collectionStatistics = collection.admin().getStatistics();

        assertThat("max is set correctly in collection statistics", (Integer) collectionStatistics.get("max"),
                  is(maxDocuments));
    }

    @Test
    public void shouldGetCollectionNamesFromDatabase() {
        Set<String> collections = database.admin().getCollectionNames();

        assertThat(collections.isEmpty(), is(true));

        database.admin().createCollection("FirstCollection");
        database.admin().createCollection("SecondCollection");

        collections = database.admin().getCollectionNames();

        assertThat("Should be two collections plus the indexes namespace", collections.size(), is(3));
        assertThat(collections.contains("FirstCollection"), is(true));
        assertThat(collections.contains("SecondCollection"), is(true));
    }

}
