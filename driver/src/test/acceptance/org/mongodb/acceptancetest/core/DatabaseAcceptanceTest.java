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

package org.mongodb.acceptancetest.core;

import org.junit.Ignore;
import org.junit.Test;
import org.mongodb.CreateCollectionOptions;
import org.mongodb.DatabaseTestCase;
import org.mongodb.Document;
import org.mongodb.MongoClient;
import org.mongodb.MongoClients;
import org.mongodb.MongoCollection;
import org.mongodb.MongoCredential;
import org.mongodb.ReadPreference;
import org.mongodb.command.RenameCollectionOptions;
import org.mongodb.connection.Connection;
import org.mongodb.connection.MongoSecurityException;
import org.mongodb.operation.MongoServerException;
import org.mongodb.operation.ReadPreferenceServerSelector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mongodb.Fixture.getCluster;
import static org.mongodb.Fixture.getCredentialList;
import static org.mongodb.Fixture.getOptions;
import static org.mongodb.Fixture.getPrimary;

/**
 * Documents the basic functionality available for Databases via the Java driver.
 */
public class DatabaseAcceptanceTest extends DatabaseTestCase {
    @Test
    public void shouldCreateCollection() {
        database.tools().createCollection(getCollectionName());

        final Set<String> collections = database.tools().getCollectionNames();
        assertThat(collections.contains(getCollectionName()), is(true));
    }

    @Test
    public void shouldCreateCappedCollection() {
        database.tools().createCollection(new CreateCollectionOptions(getCollectionName(), true, 40 * 1024));

        final Set<String> collections = database.tools().getCollectionNames();
        assertThat(collections.contains(getCollectionName()), is(true));

        final MongoCollection<Document> collection = database.getCollection(getCollectionName());
        assertThat(collection.tools().isCapped(), is(true));

        assertThat("Should have the default index on _id", collection.tools().getIndexes().size(), is(1));
    }

    @Test
    public void shouldCreateCappedCollectionWithoutAutoIndex() {
        database.tools().createCollection(new CreateCollectionOptions(getCollectionName(), true, 40 * 1024, false));

        final Set<String> collections = database.tools().getCollectionNames();
        assertThat(collections.contains(getCollectionName()), is(true));

        final MongoCollection<Document> collection = database.getCollection(getCollectionName());
        assertThat(collection.tools().isCapped(), is(true));

        assertThat("Should NOT have the default index on _id", collection.tools().getIndexes().size(), is(0));
    }

    @Test
    public void shouldSupportMaxNumberOfDocumentsInACappedCollection() {
        final int maxDocuments = 5;
        database.tools()
                .createCollection(new CreateCollectionOptions(getCollectionName(), true, 40 * 1024, false, maxDocuments));

        final Set<String> collections = database.tools().getCollectionNames();
        assertThat(collections.contains(getCollectionName()), is(true));

        final MongoCollection<Document> collection = database.getCollection(getCollectionName());
        final Document collectionStatistics = collection.tools().getStatistics();

        assertThat("max is set correctly in collection statistics", (Integer) collectionStatistics.get("max"),
                is(maxDocuments));
    }

    @Test
    public void shouldGetCollectionNamesFromDatabase() {
        database.tools().createCollection(getCollectionName());

        final Set<String> collections = database.tools().getCollectionNames();

        assertThat(collections.contains("system.indexes"), is(true));
        assertThat(collections.contains(getCollectionName()), is(true));
    }

    @Test
    public void shouldChangeACollectionNameWhenRenameIsCalled() {
        //given
        final String originalCollectionName = "originalCollection";
        final MongoCollection<Document> originalCollection = database.getCollection(originalCollectionName);
        originalCollection.insert(new Document("someKey", "someValue"));

        assertThat(database.tools().getCollectionNames().contains(originalCollectionName), is(true));

        //when
        final String newCollectionName = "TheNewCollectionName";
        database.tools().renameCollection(originalCollectionName, newCollectionName);

        //then
        assertThat(database.tools().getCollectionNames().contains(originalCollectionName), is(false));
        assertThat(database.tools().getCollectionNames().contains(newCollectionName), is(true));

        final MongoCollection<Document> renamedCollection = database.getCollection(newCollectionName);
        assertThat("Renamed collection should have the same number of documents as original",
                renamedCollection.find().count(), is(1L));
    }

    @Test(expected = MongoServerException.class)
    public void shouldNotBeAbleToRenameACollectionToAnExistingCollectionName() {
        // TODO - maybe this needs to be an exception that maps directly onto error code 10027?
        // Otherwise we can have false positives

        //given
        final String originalCollectionName = "originalCollectionToRename";
        database.tools().createCollection(originalCollectionName);

        final String anotherCollectionName = "anExistingCollection";
        database.tools().createCollection(anotherCollectionName);

        assertThat(database.tools().getCollectionNames().contains(anotherCollectionName), is(true));
        assertThat(database.tools().getCollectionNames().contains(originalCollectionName), is(true));

        //when
        database.tools().renameCollection(getCollectionName(), anotherCollectionName);
    }

    @Test
    public void shouldBeAbleToRenameCollectionToAnExistingCollectionNameAndReplaceItWhenDropIsTrue() {
        //given
        final String existingCollectionName = "anExistingCollection";
        final String originalCollectionName = "someOriginalCollection";

        final MongoCollection<Document> originalCollection = database.getCollection(originalCollectionName);
        final String keyInOriginalCollection = "someKey";
        final String valueInOriginalCollection = "someValue";
        originalCollection.insert(new Document(keyInOriginalCollection, valueInOriginalCollection));

        final MongoCollection<Document> existingCollection = database.getCollection(existingCollectionName);
        final String keyInExistingCollection = "aDifferentDocument";
        final String valueInExistingCollection = "withADifferentValue";
        existingCollection.insert(new Document(keyInExistingCollection, valueInExistingCollection));

        assertThat(database.tools().getCollectionNames().contains(originalCollectionName), is(true));
        assertThat(database.tools().getCollectionNames().contains(existingCollectionName), is(true));

        //when
        database.tools().renameCollection(new RenameCollectionOptions(originalCollectionName,
                existingCollectionName, true));

        //then
        assertThat(database.tools().getCollectionNames().contains(originalCollectionName), is(false));
        assertThat(database.tools().getCollectionNames().contains(existingCollectionName), is(true));

        final MongoCollection<Document> replacedCollection = database.getCollection(existingCollectionName);
        assertThat(replacedCollection.find().getOne().get(keyInExistingCollection), is(nullValue()));
        assertThat(replacedCollection.find().getOne().get(keyInOriginalCollection).toString(), is(valueInOriginalCollection));
    }

    @Test
    @Ignore("Failing in v2.5.2-pre-")
    public void shouldBeAbleToAuthenticateAfterAddingUser() throws InterruptedException {
        //TODO Cleanup needed. connection and newCredentialList are redundant.
        MongoCredential credential = MongoCredential.createMongoCRCredential("xx", getDatabaseName(), "e".toCharArray());
        Connection connection = getCluster().getServer(new ReadPreferenceServerSelector(ReadPreference.primary())).getConnection();
        try {
            database.tools().addUser(credential.getUserName(), credential.getPassword(), true);
            List<MongoCredential> newCredentialList = new ArrayList<MongoCredential>(getCredentialList());
            newCredentialList.add(credential);
            MongoClient client = MongoClients.create(getPrimary(), Arrays.asList(credential), getOptions());
            client.getDatabase(database.getName()).getCollection(getCollectionName()).find().getOne();
            // implicitly, we're asserting that authenticate does not throw an exception, which would happen if auth failed./
        } finally {
            database.tools().removeUser(credential.getUserName());
            connection.close();
        }
    }

    @Test
    public void shouldNotBeAbleToAuthenticateAfterRemovingUser() throws InterruptedException {
        MongoCredential credential = MongoCredential.createMongoCRCredential("xx", getDatabaseName(), "e".toCharArray());
        Connection connection = getCluster().getServer(new ReadPreferenceServerSelector(ReadPreference.primary())).getConnection();
        try {
            database.tools().addUser(credential.getUserName(), credential.getPassword(), true);
            database.tools().removeUser(credential.getUserName());
            try {
                MongoClient client = MongoClients.create(getPrimary(), Arrays.asList(credential), getOptions());
                client.getDatabase("test").getCollection("test").find().getOne();
            } catch (MongoSecurityException e) {
                // all good.  using this style to make sure that it's not the addUser call that is throwing.  of course, could move
                // the addUser to setUp, but that would require its own test class.
            }
        } finally {
            connection.close();
        }
    }

    @Test
    @Ignore("not implemented")
    public void shouldFailRenameIfSharded() {

    }
}
