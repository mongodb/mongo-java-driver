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

package org.mongodb.acceptancetest.crud;

import org.bson.types.Document;
import org.junit.Ignore;
import org.junit.Test;
import org.mongodb.CreateCollectionOptions;
import org.mongodb.DatabaseTestCase;
import org.mongodb.MongoCollection;
import org.mongodb.MongoServerException;
import org.mongodb.command.RenameCollectionOptions;

import java.util.Set;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Documents the basic functionality available for Databases via the Java driver.
 */
public class DatabaseAcceptanceTest extends DatabaseTestCase {
    @Test
    public void shouldCreateCollection() {
        database.admin().createCollection(collectionName);

        final Set<String> collections = database.admin().getCollectionNames();
        assertThat(collections.contains(collectionName), is(true));
    }

    @Test
    public void shouldCreateCappedCollection() {
        database.admin().createCollection(new CreateCollectionOptions(collectionName, true, 40 * 1024));

        final Set<String> collections = database.admin().getCollectionNames();
        assertThat(collections.contains(collectionName), is(true));

        final MongoCollection<Document> collection = database.getCollection(collectionName);
        assertThat(collection.admin().isCapped(), is(true));

        assertThat("Should have the default index on _id", collection.admin().getIndexes().size(), is(1));
    }

    @Test
    public void shouldCreateCappedCollectionWithoutAutoIndex() {
        database.admin().createCollection(new CreateCollectionOptions(collectionName, true, 40 * 1024, false));

        final Set<String> collections = database.admin().getCollectionNames();
        assertThat(collections.contains(collectionName), is(true));

        final MongoCollection<Document> collection = database.getCollection(collectionName);
        assertThat(collection.admin().isCapped(), is(true));

        assertThat("Should NOT have the default index on _id", collection.admin().getIndexes().size(), is(0));
    }

    @Test
    public void shouldSupportMaxNumberOfDocumentsInACappedCollection() {
        final int maxDocuments = 5;
        database.admin()
                .createCollection(new CreateCollectionOptions(collectionName, true, 40 * 1024, false, maxDocuments));

        final Set<String> collections = database.admin().getCollectionNames();
        assertThat(collections.contains(collectionName), is(true));

        final MongoCollection<Document> collection = database.getCollection(collectionName);
        final Document collectionStatistics = collection.admin().getStatistics();

        assertThat("max is set correctly in collection statistics", (Integer) collectionStatistics.get("max"),
                  is(maxDocuments));
    }

    @Test
    public void shouldGetCollectionNamesFromDatabase() {
        database.admin().createCollection(collectionName);

        final Set<String> collections = database.admin().getCollectionNames();

        assertThat(collections.contains("system.indexes"), is(true));
        assertThat(collections.contains(collectionName), is(true));
    }

    @Test
    public void shouldChangeACollectionNameWhenRenameIsCalled() {
        //given
        final String originalCollectionName = "originalCollection";
        final MongoCollection<Document> originalCollection = database.getCollection(originalCollectionName);
        originalCollection.insert(new Document("someKey", "someValue"));

        assertThat(database.admin().getCollectionNames().contains(originalCollectionName), is(true));

        //when
        final String newCollectionName = "TheNewCollectionName";
        database.admin().renameCollection(originalCollectionName, newCollectionName);

        //then
        assertThat(database.admin().getCollectionNames().contains(originalCollectionName), is(false));
        assertThat(database.admin().getCollectionNames().contains(newCollectionName), is(true));

        final MongoCollection<Document> renamedCollection = database.getCollection(newCollectionName);
        assertThat("Renamed collection should have the same number of documents as original",
                  renamedCollection.count(), is(1L));
    }

    @Test(expected = MongoServerException.class)
    public void shouldNotBeAbleToRenameACollectionToAnExistingCollectionName() {
        // TODO - maybe this needs to be an exception that maps directly onto error code 10027?
        // Otherwise we can have false positives

        //given
        final String originalCollectionName = "originalCollectionToRename";
        database.admin().createCollection(originalCollectionName);

        final String anotherCollectionName = "anExistingCollection";
        database.admin().createCollection(anotherCollectionName);

        assertThat(database.admin().getCollectionNames().contains(anotherCollectionName), is(true));
        assertThat(database.admin().getCollectionNames().contains(originalCollectionName), is(true));

        //when
        database.admin().renameCollection(collectionName, anotherCollectionName);
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

        assertThat(database.admin().getCollectionNames().contains(originalCollectionName), is(true));
        assertThat(database.admin().getCollectionNames().contains(existingCollectionName), is(true));

        //when
        database.admin().renameCollection(new RenameCollectionOptions(originalCollectionName,
                                                                     existingCollectionName, true));

        //then
        assertThat(database.admin().getCollectionNames().contains(originalCollectionName), is(false));
        assertThat(database.admin().getCollectionNames().contains(existingCollectionName), is(true));

        final MongoCollection<Document> replacedCollection = database.getCollection(existingCollectionName);
        assertThat(replacedCollection.one().get(keyInExistingCollection), is(nullValue()));
        assertThat(replacedCollection.one().get(keyInOriginalCollection).toString(), is(valueInOriginalCollection));
    }

    @Test
    @Ignore("not implemented")
    public void shouldFailRenameIfSharded() {

    }
}
