package org.mongodb.acceptancetest.crud;

import org.bson.types.Document;
import org.junit.Before;
import org.junit.Test;
import org.mongodb.MongoClient;
import org.mongodb.MongoCollection;
import org.mongodb.MongoDatabase;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mongodb.acceptancetest.Fixture.createMongoClient;

/**
 * Documents the basic functionality of MongoDB Collections available via the Java driver.
 */
public class CollectionAcceptanceTest {
    private static final String DB_NAME = "CollectionAcceptanceTest";
    private MongoDatabase database;

    @Before
    public void setUp() {
        final MongoClient mongoClient = createMongoClient();

        database = mongoClient.getDatabase(DB_NAME);
        database.admin().drop();

    }

    @Test
    public void shouldCountNumberOfDocumentsInCollection() {
        final MongoCollection<Document> collection = database.getCollection("collection");
        assertThat(collection.count(), is(0L));

        collection.insert(new Document("myField", "myValue"));

        assertThat(collection.count(), is(1L));
    }

    @Test
    public void shouldGetStatistics() {
        final String newCollectionName = "shouldGetStatistics";
        database.admin().createCollection(newCollectionName);
        final MongoCollection<Document> collection = database.getCollection(newCollectionName);

        Document collectionStatistics = collection.admin().getStatistics();
        assertThat(collectionStatistics, is(notNullValue()));
        assertThat((String) collectionStatistics.get("ns"), is(DB_NAME + "." + newCollectionName));
    }

    @Test
    public void shouldDropExistingCollection() {
        final String collectionName = "shouldDropExistingCollection";
        database.admin().createCollection(collectionName);
        final MongoCollection<Document> newCollection = database.getCollection(collectionName);

        assertThat(database.admin().getCollectionNames().contains(collectionName), is(true));

        newCollection.admin().drop();

        assertThat(database.admin().getCollectionNames().contains(collectionName), is(false));
    }

}
