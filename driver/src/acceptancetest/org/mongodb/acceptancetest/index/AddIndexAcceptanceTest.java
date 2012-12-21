package org.mongodb.acceptancetest.index;

import org.bson.types.Document;
import org.junit.Before;
import org.junit.Test;
import org.mongodb.Index;
import org.mongodb.MongoClient;
import org.mongodb.MongoCollection;
import org.mongodb.MongoDatabase;
import org.mongodb.OrderBy;
import org.mongodb.operation.MongoInsert;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mongodb.OrderBy.ASC;
import static org.mongodb.OrderBy.DESC;
import static org.mongodb.OrderBy.fromInt;
import static org.mongodb.acceptancetest.Fixture.createMongoClient;

public class AddIndexAcceptanceTest {
    private static final String DB_NAME = "AddIndexAcceptanceTest";
    private MongoCollection<Document> collection;

    @Before
    public void setUp() {
        final MongoClient mongoClient = createMongoClient();

        final MongoDatabase database = mongoClient.getDatabase(DB_NAME);
        database.admin().drop();

        collection = database.getCollection("collection");

        assertThat("Should be no indexes on the database at all at this stage", collection.admin().getIndexes().size(),
                   is(0));
    }

    @Test
    public void shouldGetExistingIndexesOnDatabase() {
        collection.insert(new MongoInsert<Document>(new Document("new", "value")));

        assertThat("Should have the default index on _id when a document exists",
                   collection.admin().getIndexes().size(), is(1));
        String nameOfIndex = (String) collection.admin().getIndexes().get(0).get("name");
        assertThat("Should be the default index on id", nameOfIndex, is("_id_"));
    }

    @Test
    public void shouldCreateIndexOnCollectionWithoutIndex() {
        final Index index = new Index("theField");
        collection.admin().ensureIndex(index);

        assertThat("Should be default index and new index on the database now", collection.admin().getIndexes().size(),
                   is(2));
    }

    @Test
    public void shouldCreateIndexWithNameOfFieldPlusOrder() {
        final Index index = new Index("theField", ASC);
        collection.admin().ensureIndex(index);

        String nameOfCreatedIndex = (String) collection.admin().getIndexes().get(1).get("name");
        assertThat("Should be an index with name of field, ascending", nameOfCreatedIndex, is("theField_1"));
    }

    @Test
    public void shouldCreateAscendingIndexByDefault() {
        final Index index = new Index("theFieldToIndex");
        collection.admin().ensureIndex(index);

        Document newIndexDetails = collection.admin().getIndexes().get(1);
        OrderBy order = fromInt((Integer) ((Document) newIndexDetails.get("key")).get("theFieldToIndex"));
        assertThat("Index created should be ascending", order, is(ASC));
    }

    @Test
    public void shouldCreateAnAscendingIndex() {
        final Index index = new Index("field", ASC);
        collection.admin().ensureIndex(index);

        Document newIndexDetails = collection.admin().getIndexes().get(1);
        OrderBy order = fromInt((Integer) ((Document) newIndexDetails.get("key")).get("field"));
        assertThat("Index created should be ascending", order, is(ASC));
    }

    @Test
    public void shouldCreateADescendingIndex() {
        final Index index = new Index("field", DESC);
        collection.admin().ensureIndex(index);

        Document newIndexDetails = collection.admin().getIndexes().get(1);
        OrderBy order = fromInt((Integer) ((Document) newIndexDetails.get("key")).get("field"));
        assertThat("Index created should be descending", order, is(DESC));
    }

    @Test
    public void shouldCreateNonUniqueIndexByDefault() {
        final Index index = new Index("field", DESC);
        collection.admin().ensureIndex(index);

        Document newIndexDetails = collection.admin().getIndexes().get(1);
        assertThat("Index created should not be unique", newIndexDetails.get("unique"), is(nullValue()));
    }

    @Test
    public void shouldCreateIndexOfUniqueValues() {
        final Index index = new Index("field", DESC, true);
        collection.admin().ensureIndex(index);

        Document newIndexDetails = collection.admin().getIndexes().get(1);
        Boolean unique = (Boolean) newIndexDetails.get("unique");
        assertThat("Index created should be unique", unique, is(true));
    }

    @Test
    public void shouldSupportCompoundIndexes() {
        final Index index = new Index("theFirstField", "theSecondField");
        collection.admin().ensureIndex(index);

        Document newIndexDetails = collection.admin().getIndexes().get(1);

        Document keys = (Document) newIndexDetails.get("key");
        Object theFirstField = keys.get("theFirstField");
        assertThat("Index should contain the first key", theFirstField, is(notNullValue()));
        OrderBy orderBy = fromInt((Integer) theFirstField);
        assertThat("Index created should be ascending", orderBy, is(ASC));

        Object theSecondField = keys.get("theSecondField");
        assertThat("Index should contain the second key", theSecondField, is(notNullValue()));
        orderBy = fromInt((Integer) theSecondField);
        assertThat("Index created should be ascending", orderBy, is(ASC));

        assertThat("Index name should contain both field names", (String) newIndexDetails.get("name"),
                   is("theFirstField_1_theSecondField_1"));
    }

    @Test
    public void shouldSupportCompoundIndexesWithDifferentOrders() {
        final Index index = new Index(new Index.Key("theFirstField", ASC), new Index.Key("theSecondField", DESC));
        collection.admin().ensureIndex(index);

        Document newIndexDetails = collection.admin().getIndexes().get(1);

        Document keys = (Document) newIndexDetails.get("key");

        OrderBy orderBy = fromInt((Integer) keys.get("theFirstField"));
        assertThat("First index should be ascending", orderBy, is(ASC));

        orderBy = fromInt((Integer) keys.get("theSecondField"));
        assertThat("Second index should be descending", orderBy, is(DESC));

        assertThat("Index name should contain both field names", (String) newIndexDetails.get("name"),
                   is("theFirstField_1_theSecondField_-1"));
    }

    //TODO: sparse
    //TODO: other ordering options
    //TODO: can you disable the index on ID for non-capped collections?
    //TODO: test to prove that all indexes for the DB are coming back, not just those for the collection
}
