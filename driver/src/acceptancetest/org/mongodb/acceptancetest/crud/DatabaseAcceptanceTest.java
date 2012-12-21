package org.mongodb.acceptancetest.crud;

import org.bson.types.Document;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mongodb.MongoClient;
import org.mongodb.MongoCollection;
import org.mongodb.MongoDatabase;

import java.util.Set;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mongodb.acceptancetest.Fixture.createMongoClient;

public class DatabaseAcceptanceTest {
    private static final String DB_NAME = "DatabaseAcceptanceTest";
    private MongoDatabase database;

    @Before
    public void setUp() {
        final MongoClient mongoClient = createMongoClient();

        database = mongoClient.getDatabase(DB_NAME);
        database.admin().drop();
    }

    @Test
    public void shouldCreateCollection() {
        String collectionName = "newCollectionName";
        database.admin().createCollection(collectionName);

        Set<String> collections = database.admin().getCollectionNames();
        assertThat("The new collection should exist on the database", collections.size(), is(2));
        assertThat(collections.contains("newCollectionName"), is(true));
    }

    @Test
    public void shouldCreateCappedCollection() {
        String collectionName = "newCollectionName";
        database.admin().createCollection(collectionName, true, 40 * 1024);

        Set<String> collections = database.admin().getCollectionNames();
        assertThat(collections.contains("newCollectionName"), is(true));

        MongoCollection<Document> collection = database.getCollection(collectionName);
        assertThat(collection.admin().isCapped(), is(true));

        assertThat("Should have the default index on _id", collection.admin().getIndexes().size(), is(1));
    }

    @Test
    public void shouldCreateCappedCollectionWithoutAutoIndex() {
        String collectionName = "newCollectionName";
        database.admin().createCollection(collectionName, true, 40 * 1024, false);

        Set<String> collections = database.admin().getCollectionNames();
        assertThat(collections.contains("newCollectionName"), is(true));

        MongoCollection<Document> collection = database.getCollection(collectionName);
        assertThat(collection.admin().isCapped(), is(true));

        assertThat("Should NOT have the default index on _id", collection.admin().getIndexes().size(), is(0));
    }

    @Test
    @Ignore("This test is here to remind us that this functionality has not been implemented yet and is required")
    public void shouldSupportMaxNumberOfDocumentsInACappedCollection() {
        Assert.fail("functionality has not been implemented for this yet");
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
