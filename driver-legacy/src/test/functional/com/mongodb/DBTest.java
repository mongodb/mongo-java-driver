/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb;

import com.mongodb.client.model.Collation;
import com.mongodb.client.model.CollationAlternate;
import com.mongodb.client.model.CollationCaseFirst;
import com.mongodb.client.model.CollationMaxVariable;
import com.mongodb.client.model.CollationStrength;
import com.mongodb.internal.operation.ListCollectionsOperation;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.codecs.BsonDocumentCodec;
import org.junit.Test;

import static com.mongodb.ClusterFixture.disableMaxTimeFailPoint;
import static com.mongodb.ClusterFixture.enableMaxTimeFailPoint;
import static com.mongodb.ClusterFixture.getBinding;
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.ClusterFixture.isSharded;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.DBObjectMatchers.hasFields;
import static com.mongodb.DBObjectMatchers.hasSubdocument;
import static com.mongodb.Fixture.getDefaultDatabaseName;
import static com.mongodb.Fixture.getMongoClient;
import static com.mongodb.ReadPreference.secondary;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeThat;
import static org.junit.Assume.assumeTrue;

@SuppressWarnings("deprecation")
public class DBTest extends DatabaseTestCase {
    @Test
    public void shouldGetDefaultWriteConcern() {
        assertEquals(WriteConcern.ACKNOWLEDGED, database.getWriteConcern());
    }

    @Test
    public void shouldGetDefaultReadPreference() {
        assertEquals(ReadPreference.primary(), database.getReadPreference());
    }

    @Test
    public void shouldGetMongoClient() {
        assertEquals(getMongoClient(), database.getMongoClient());
    }

    @Test
    public void shouldReturnCachedCollectionObjectIfExists() {
        DBCollection collection1 = database.getCollection("test");
        DBCollection collection2 = database.getCollection("test");
        assertThat("Checking that references are equal", collection1, sameInstance(collection2));
    }

    @Test
    public void shouldDropItself() {
        // when
        String databaseName = "drop-test-" + System.nanoTime();
        DB db = getMongoClient().getDB(databaseName);
        db.createCollection(collectionName, new BasicDBObject());

        // then
        assertThat(getMongoClient().listDatabaseNames(), hasItem(databaseName));

        // when
        db.dropDatabase();

        // then
        assertThat(getMongoClient().listDatabaseNames(), not(hasItem(databaseName)));
    }

    @Test
    public void shouldGetCollectionNames() {
        database.dropDatabase();

        String[] collectionNames = {"c1", "c2", "c3"};

        for (final String name : collectionNames) {
            database.createCollection(name, new BasicDBObject());
        }

        assertThat(database.getCollectionNames(), hasItems(collectionNames));
    }

    @Test
    public void shouldDeferCollectionCreationIfOptionsIsNull() {
        collection.drop();
        database.createCollection(collectionName, null);
        assertFalse(database.getCollectionNames().contains(collectionName));
    }

    @Test
    public void shouldCreateCappedCollection() {
        collection.drop();
        database.createCollection(collectionName, new BasicDBObject("capped", true)
                                                  .append("size", 242880));
        assertTrue(database.getCollection(collectionName).isCapped());
    }

    @Test
    public void shouldCreateCappedCollectionWithMaxNumberOfDocuments() {
        collection.drop();
        DBCollection cappedCollectionWithMax = database.createCollection(collectionName, new BasicDBObject("capped", true)
                                                                                         .append("size", 242880)
                                                                                         .append("max", 10));

        assertThat(cappedCollectionWithMax.getStats(), hasSubdocument(new BasicDBObject("capped", true).append("max", 10)));

        for (int i = 0; i < 11; i++) {
            cappedCollectionWithMax.insert(new BasicDBObject("x", i));
        }
        assertThat(cappedCollectionWithMax.find().count(), is(10));
    }

    @Test
    public void shouldCreateUncappedCollection() {
        collection.drop();
        BasicDBObject creationOptions = new BasicDBObject("capped", false);
        database.createCollection(collectionName, creationOptions);

        assertFalse(database.getCollection(collectionName).isCapped());
    }

    @Test(expected = MongoCommandException.class)
    public void shouldThrowErrorIfCreatingACappedCollectionWithANegativeSize() {
        collection.drop();
        DBObject creationOptions = BasicDBObjectBuilder.start().add("capped", true)
                                                       .add("size", -20).get();
        database.createCollection(collectionName, creationOptions);
    }

    @Test
    public void shouldCreateCollectionWithTheSetCollation() {
        assumeThat(serverVersionAtLeast(3, 4), is(true));
        // Given
        collection.drop();
        Collation collation = Collation.builder()
                .locale("en")
                .caseLevel(true)
                .collationCaseFirst(CollationCaseFirst.OFF)
                .collationStrength(CollationStrength.IDENTICAL)
                .numericOrdering(true)
                .collationAlternate(CollationAlternate.SHIFTED)
                .collationMaxVariable(CollationMaxVariable.SPACE)
                .backwards(true)
                .build();

        DBObject options = BasicDBObject.parse("{ collation: { locale: 'en', caseLevel: true, caseFirst: 'off', strength: 5,"
                + "numericOrdering: true, alternate: 'shifted',  maxVariable: 'space', backwards: true }}");

        // When
        database.createCollection(collectionName, options);
        BsonDocument collectionCollation = getCollectionInfo(collectionName).getDocument("options").getDocument("collation");

        // Then
        BsonDocument collationDocument = collation.asDocument();
        for (String key: collationDocument.keySet()) {
            assertEquals(collationDocument.get(key), collectionCollation.get(key));
        }

        // When - collation set on the database
        database.getCollection(collectionName).drop();
        database.createCollection(collectionName, new BasicDBObject("collation", BasicDBObject.parse(collation.asDocument().toJson())));
        collectionCollation = getCollectionInfo(collectionName).getDocument("options").getDocument("collation");

        // Then
        collationDocument = collation.asDocument();
        for (String key: collationDocument.keySet()) {
            assertEquals(collationDocument.get(key), collectionCollation.get(key));
        }
    }

    @Test(expected = DuplicateKeyException.class)
    public void shouldGetDuplicateKeyException() {
        DBObject doc = new BasicDBObject("_id", 1);
        collection.insert(doc);
        collection.insert(doc, WriteConcern.ACKNOWLEDGED);
    }

    @Test
    public void shouldExecuteCommand() {
        CommandResult commandResult = database.command(new BasicDBObject("isMaster", 1));
        assertThat(commandResult, hasFields(new String[]{"ismaster", "maxBsonObjectSize", "ok"}));
    }

    @Test(expected = MongoExecutionTimeoutException.class)
    public void shouldTimeOutCommand() {
        assumeThat(isSharded(), is(false));
        enableMaxTimeFailPoint();
        try {
            database.command(new BasicDBObject("isMaster", 1).append("maxTimeMS", 1));
        } finally {
            disableMaxTimeFailPoint();
        }
    }

    @Test
    public void shouldExecuteCommandWithReadPreference() {
        assumeTrue(isDiscoverableReplicaSet());
        CommandResult commandResult = database.command(new BasicDBObject("dbStats", 1).append("scale", 1), secondary());
        assertThat(commandResult, hasFields(new String[]{"collections", "avgObjSize", "indexes", "db", "indexSize", "storageSize"}));
    }

    @Test
    public void shouldNotThrowAnExceptionOnCommandFailure() {
        CommandResult commandResult = database.command(new BasicDBObject("nonExistentCommand", 1));
        assertThat(commandResult, hasFields(new String[]{"ok", "errmsg"}));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowAnExceptionWhenDBNameContainsSpaces() {
        getMongoClient().getDB("foo bar");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowAnExceptionWhenDBNameIsEmpty() {
        getMongoClient().getDB("");
    }

    @Test
    public void shouldIgnoreCaseWhenCheckingIfACollectionExists() {
        // Given
        database.getCollection("foo1").drop();
        assertFalse(database.collectionExists("foo1"));

        // When
        database.createCollection("foo1", new BasicDBObject());

        // Then
        assertTrue(database.collectionExists("foo1"));
        assertTrue(database.collectionExists("FOO1"));
        assertTrue(database.collectionExists("fOo1"));

        // Finally
        database.getCollection("foo1").drop();
    }

    @Test
    public void shouldReturnFailureWithErrorMessageWhenExecutingInvalidCommand() {
        assumeTrue(!isSharded());

        // When
        CommandResult commandResult = database.command(new BasicDBObject("NotRealCommandName", 1));

        // Then
        assertThat(commandResult.ok(), is(false));
        assertThat(commandResult.getErrorMessage(), containsString("no such"));
    }

    @Test
    public void shouldReturnOKWhenASimpleCommandExecutesSuccessfully() {
        // When
        CommandResult commandResult = database.command(new BasicDBObject("isMaster", 1));

        // Then
        assertThat(commandResult.ok(), is(true));
        assertThat((Boolean) commandResult.get("ismaster"), is(true));
    }

    @Test
    public void shouldRunCommandAgainstSecondaryWhenOnlySecondaryReadPreferenceSpecified() {
        assumeTrue(isDiscoverableReplicaSet());

        // When
        CommandResult commandResult = database.command(new BasicDBObject("dbstats", 1), secondary());

        // Then
        assertThat(commandResult.ok(), is(true));
        assertThat((String) commandResult.get("serverUsed"), not(containsString(":27017")));
    }

    @Test
    public void shouldRunStringCommandAgainstSecondaryWhenSecondaryReadPreferenceSpecified() {
        assumeTrue(isDiscoverableReplicaSet());

        // When
        CommandResult commandResult = database.command("dbstats", secondary());

        // Then
        assertThat(commandResult.ok(), is(true));
        assertThat((String) commandResult.get("serverUsed"), not(containsString(":27017")));
    }

    @Test
    public void shouldRunCommandAgainstSecondaryWhenOnlySecondaryReadPreferenceSpecifiedAlongWithEncoder() {
        assumeTrue(isDiscoverableReplicaSet());

        // When
        CommandResult commandResult = database.command(new BasicDBObject("dbstats", 1), secondary(), DefaultDBEncoder.FACTORY.create());

        // Then
        assertThat(commandResult.ok(), is(true));
        assertThat((String) commandResult.get("serverUsed"), not(containsString(":27017")));
    }

    BsonDocument getCollectionInfo(final String collectionName) {
        return new ListCollectionsOperation<BsonDocument>(getDefaultDatabaseName(), new BsonDocumentCodec())
                .filter(new BsonDocument("name", new BsonString(collectionName))).execute(getBinding()).next().get(0);
    }
}
