package org.mongodb.acceptancetest.crud;

import org.bson.types.Document;
import org.junit.Before;
import org.junit.Test;
import org.mongodb.MongoClient;
import org.mongodb.MongoCollection;
import org.mongodb.MongoDatabase;
import org.mongodb.operation.MongoInsert;

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
    public void shouldGetCollectionNamesFromDatabase() {
        Set<String> collections = database.admin().getCollectionNames();

        assertThat(collections.isEmpty(), is(true));

        createCollection(database, "FirstCollection");
        createCollection(database, "SecondCollection");

        collections = database.admin().getCollectionNames();

        assertThat("Should be two collections plus the indexes namespace", collections.size(), is(3));
        assertThat(collections.contains("FirstCollection"), is(true));
        assertThat(collections.contains("SecondCollection"), is(true));
    }

    private void createCollection(final MongoDatabase mongoDatabase, final String collectionName) {
        final MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
        collection.insert(new MongoInsert<Document>(new Document("field", 1)));
    }

}
