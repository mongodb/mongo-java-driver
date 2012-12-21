package org.mongodb.acceptancetest.crud;

import org.bson.types.Document;
import org.junit.Before;
import org.junit.Test;
import org.mongodb.MongoClient;
import org.mongodb.MongoCollection;
import org.mongodb.MongoDatabase;
import org.mongodb.operation.MongoInsert;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mongodb.acceptancetest.Fixture.createMongoClient;

public class CollectionAcceptanceTest {
    private static final String DB_NAME = "CollectionAcceptanceTest";
    private MongoCollection<Document> collection;

    @Before
    public void setUp() {
        final MongoClient mongoClient = createMongoClient();

        final MongoDatabase database = mongoClient.getDatabase(DB_NAME);
        database.admin().drop();

        collection = database.getCollection("collection");
    }

    @Test
    public void shouldCountNumberOfDocumentsInCollection() {
        assertThat(collection.count(), is(0L));

        collection.insert(new MongoInsert<Document>(new Document("myField", "myValue")));

        assertThat(collection.count(), is(1L));
    }

    @Test
    public void shouldGetStatistics() {
        collection.getDatabase().admin().createCollection(collection.getName());
        Document collectionStatistics = collection.admin().getStatistics();
        assertThat(collectionStatistics, is(notNullValue()));
        assertThat((String) collectionStatistics.get("ns"), is(DB_NAME + ".collection"));
    }

}
