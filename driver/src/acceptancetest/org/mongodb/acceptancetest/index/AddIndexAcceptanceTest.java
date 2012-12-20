package org.mongodb.acceptancetest.index;

import org.bson.types.Document;
import org.junit.Before;
import org.junit.Test;
import org.mongodb.MongoClient;
import org.mongodb.MongoCollection;
import org.mongodb.MongoDatabase;
import org.mongodb.OrderBy;
import org.mongodb.operation.MongoInsert;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mongodb.acceptancetest.Fixture.createMongoClient;
import static org.mongodb.OrderBy.ASC;
import static org.mongodb.OrderBy.DESC;

public class AddIndexAcceptanceTest {
    private static final String DB_NAME = "AddIndexAcceptanceTest";
    private MongoCollection<Document> collection;

    @Before
    public void setUp() {
        final MongoClient mongoClient = createMongoClient();

        final MongoDatabase database = mongoClient.getDatabase(DB_NAME);
        database.admin().drop();

        collection = database.getCollection("collection");
    }

    @Test
    public void shouldGetExistingIndexesOnDatabase() {
        assertThat("Should be no indexes on the database at all at this stage", collection.admin().getIndexes().size(),
                   is(0));

        collection.insert(new MongoInsert<Document>(new Document("new", "value")));

        assertThat("Should have the default index on _id", collection.admin().getIndexes().size(), is(1));
        String nameOfIndex = (String) collection.admin().getIndexes().get(0).get("name");
        assertThat("Should be the default index on id", nameOfIndex, is("_id_"));
    }

    @Test
    public void shouldCreateIndexOnCollectionWithoutIndex() {
        assertThat("Should be no indexes on the database at all at this stage", collection.admin().getIndexes().size(),
                   is(0));

        collection.insert(new MongoInsert<Document>(new Document("theField", "yourName")));
        collection.admin().ensureIndex("theField", OrderBy.ASC);

        assertThat("Should be default index and new index on the database now", collection.admin().getIndexes().size(),
                   is(2));
    }

    @Test
    public void shouldCreateIndexWithNameOfFieldPlusOrder() {
        assertThat("Should be no indexes on the database at all at this stage", collection.admin().getIndexes().size(),
                   is(0));

        collection.insert(new MongoInsert<Document>(new Document("theField", "yourName")));
        collection.admin().ensureIndex("theField", OrderBy.ASC);

        String nameOfCreatedIndex = (String) collection.admin().getIndexes().get(1).get("name");
        assertThat("Should be an index with name of field, ascending", nameOfCreatedIndex, is("theField_1"));
    }

    @Test
    public void shouldCreateAnAscendingIndex() {
        assertThat("Should be no indexes on the database at all at this stage", collection.admin().getIndexes().size(),
                   is(0));

        collection.insert(new MongoInsert<Document>(new Document("field", "yourName")));
        collection.admin().ensureIndex("field", OrderBy.ASC);

        Document newIndexDetails = collection.admin().getIndexes().get(1);
        OrderBy order = OrderBy.fromInt((Integer) ((Document) newIndexDetails.get("key")).get("field"));
        assertThat("Index created should be ascending", order, is(ASC));
    }

    @Test
    public void shouldCreateADescendingIndex() {
        assertThat("Should be no indexes on the database at all at this stage", collection.admin().getIndexes().size(),
                   is(0));

        collection.insert(new MongoInsert<Document>(new Document("field", "yourName")));
        collection.admin().ensureIndex("field", DESC);

        Document newIndexDetails = collection.admin().getIndexes().get(1);
        OrderBy order = OrderBy.fromInt((Integer) ((Document) newIndexDetails.get("key")).get("field"));
        assertThat("Index created should be descending", order, is(DESC));
    }
}
