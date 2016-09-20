/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

import com.mongodb.util.TestCase;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Ignore;
import org.junit.Test;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

import static com.mongodb.Fixture.getPrimaryAsString;
import static com.mongodb.Fixture.isAuthenticated;
import static com.mongodb.ReadPreference.primary;
import static com.mongodb.ReadPreference.primaryPreferred;
import static com.mongodb.ReadPreference.secondary;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

public class DBTest extends TestCase {

    @Test
    public void shouldIgnoreCaseWhenCheckingIfACollectionExists() {
        // Given
        getDatabase().getCollection("foo1").drop();
        assertFalse(getDatabase().collectionExists("foo1"));

        // When
        getDatabase().createCollection("foo1", new BasicDBObject());

        // Then
        assertTrue(getDatabase().collectionExists("foo1"));
        assertTrue(getDatabase().collectionExists("FOO1"));
        assertTrue(getDatabase().collectionExists("fOo1"));

        // Finally
        getDatabase().getCollection("foo1").drop();
    }

    @Test
    public void shouldReturnCorrectValueForReadPreferenceDependingUponTheCommand() throws UnknownHostException {
        assumeTrue(isReplicaSet(getMongoClient()));

        Mongo mongo = new MongoClient(new MongoClientURI("mongodb://localhost:27017,localhost:27018,localhost:27019"));


        DB db = mongo.getDB(getDatabase().getName());

        DBObject obj = new BasicDBObject("mapreduce", 1).append("out", "myColl");
        assertEquals(primary(), db.getCommandReadPreference(obj, secondary()));

        obj = new BasicDBObject("mapreduce", 1).append("out", new BasicDBObject("replace", "myColl"));
        assertEquals(primary(), db.getCommandReadPreference(obj, secondary()));

        obj = new BasicDBObject("mapreduce", 1).append("out", new BasicDBObject("inline", 1));
        assertEquals(secondary(), db.getCommandReadPreference(obj, secondary()));

        obj = new BasicDBObject("mapreduce", 1).append("out", new BasicDBObject("inline", null));
        assertEquals(primary(), db.getCommandReadPreference(obj, secondary()));

        obj = new BasicDBObject("getnonce", 1);
        assertEquals(primaryPreferred(), db.getCommandReadPreference(obj, secondary()));

        obj = new BasicDBObject("authenticate", 1);
        assertEquals(primaryPreferred(), db.getCommandReadPreference(obj, secondary()));

        obj = new BasicDBObject("count", 1);
        assertEquals(secondary(), db.getCommandReadPreference(obj, secondary()));

        obj = new BasicDBObject("count", 1);
        assertEquals(secondary(), db.getCommandReadPreference(obj, secondary()));

        obj = new BasicDBObject("serverStatus", 1);
        assertEquals(primary(), db.getCommandReadPreference(obj, secondary()));

        obj = new BasicDBObject("count", 1);
        assertEquals(primary(), db.getCommandReadPreference(obj, null));

        obj = new BasicDBObject("collStats", 1);
        assertEquals(ReadPreference.secondaryPreferred(), db.getCommandReadPreference(obj, ReadPreference.secondaryPreferred()));

        obj = new BasicDBObject("text", 1);
        assertEquals(ReadPreference.secondaryPreferred(), db.getCommandReadPreference(obj, ReadPreference.secondaryPreferred()));

        obj = new BasicDBObject("parallelCollectionScan", 1);
        assertEquals(ReadPreference.secondaryPreferred(), db.getCommandReadPreference(obj, ReadPreference.secondaryPreferred()));

        obj = new BasicDBObject("listIndexes", 1);
        assertEquals(ReadPreference.secondaryPreferred(), db.getCommandReadPreference(obj, ReadPreference.secondaryPreferred()));

        obj = new BasicDBObject("listCollections", 1);
        assertEquals(ReadPreference.secondaryPreferred(), db.getCommandReadPreference(obj, ReadPreference.secondaryPreferred()));
    }

    @Test
    public void shouldNotThrowAnErrorWhenEnsureConnectionCalledAfterRequestStart() throws UnknownHostException {

        Mongo m = new MongoClient(Arrays.asList(new ServerAddress("localhost")));

        assumeFalse(isStandalone(cleanupMongo));
        try {
            DB db = m.getDB("com_mongodb_unittest_DBTest");
            db.requestStart();
            try {
                db.requestEnsureConnection();
            } finally {
                db.requestDone();
            }
        } finally {
            m.close();
        }
    }

    @Test
    public void shouldReleaseConnectionOnLastCallToRequestEndWhenRequestStartCallsAreNested() throws UnknownHostException {
        Mongo m = new MongoClient(Arrays.asList(new ServerAddress("localhost")),
                                  MongoClientOptions.builder().connectionsPerHost(1).maxWaitTime(10000).build());
        DB db = m.getDB("com_mongodb_unittest_DBTest");

        try {
            db.requestStart();
            try {
                db.command(new BasicDBObject("ping", 1));
                db.requestStart();
                try {
                    db.command(new BasicDBObject("ping", 1));
                } finally {
                    db.requestDone();
                }
            } finally {
                db.requestDone();
            }
        } finally {
            m.close();
        }
    }

    @Test
    public void shouldReturnFailureWithErrorMessageWhenExecutingInvalidCommand() {
        // mongos 2.2 sets the query failure bit for unrecognized commands
        assumeFalse(isSharded(getMongoClient()) && !serverIsAtLeastVersion(2.4));

        // Given
        DB database = getDatabase();

        // When
        CommandResult commandResult = database.command(new BasicDBObject("NotRealCommandName", 1));

        // Then
        assertThat(commandResult.ok(), is(false));
        assertThat(commandResult.getErrorMessage(), containsString("no such"));
        assertThat(commandResult.getErrorMessage(), containsString("NotRealCommandName"));
    }

    @Test
    public void shouldReturnOKWhenASimpleCommandExecutesSuccessfully() {
        // Given
        DB database = getDatabase();

        // When
        CommandResult commandResult = database.command(new BasicDBObject("isMaster", 1));

        // Then
        assertThat(commandResult.ok(), is(true));
        assertThat((Boolean) commandResult.get("ismaster"), is(true));
    }

    @Test
    @SuppressWarnings("deprecation") // this functionality needs testing, but the tests will be removed/replaced in 3.0
    public void shouldRunCommandAgainstSecondaryWhenSlaveOkOrReadPreferenceSecondaryOrBothAreBothSet() throws UnknownHostException {
        // Given
        assumeTrue(isReplicaSet(cleanupMongo));
        DB database = getReplicaSetDB();

        try {
            //Sadly yes, this does test more than one thing.  But the overall goal is the same
            // When
            CommandResult commandResult = database.command(new BasicDBObject("dbstats", 1), Bytes.QUERYOPTION_SLAVEOK, secondary());
            // Then
            assertThat(commandResult.ok(), is(true));
            assertThat((String) commandResult.get("serverUsed"), not(containsString(getPrimaryAsString())));

            // When
            commandResult = database.command(new BasicDBObject("dbstats", 1), 0, secondary());
            // Then
            assertThat(commandResult.ok(), is(true));
            assertThat((String) commandResult.get("serverUsed"), not(containsString(getPrimaryAsString())));

            // When
            commandResult = database.command(new BasicDBObject("dbstats", 1), Bytes.QUERYOPTION_SLAVEOK, primary());
            // Then
            assertThat(commandResult.ok(), is(true));
            assertThat((String) commandResult.get("serverUsed"), not(containsString(getPrimaryAsString())));
        } finally {
            database.dropDatabase();
        }
    }

    @Test
    public void shouldRunCommandAgainstSecondaryWhenOnlySecondaryReadPreferenceSpecified() throws UnknownHostException {
        // Given
        assumeTrue(isReplicaSet(cleanupMongo));
        DB database = getReplicaSetDB();

        // When
        CommandResult commandResult = database.command(new BasicDBObject("dbstats", 1), secondary());

        // Then
        assertThat(commandResult.ok(), is(true));
        assertThat((String) commandResult.get("serverUsed"), not(containsString(getPrimaryAsString())));
    }

    @Test
    @SuppressWarnings("deprecation") // this functionality needs testing, but the tests will be removed/replaced in 3.0
    @Ignore("Intermittent, not sure this is doing what it thinks it is")
    public void shouldRunStringCommandAgainstSecondaryWhenSlaveOkOptionSpecified() throws UnknownHostException {
        // Given
        assumeTrue(isReplicaSet(cleanupMongo));
        DB database = getReplicaSetDB();

        // When
        CommandResult commandResult = database.command("dbstats", Bytes.QUERYOPTION_SLAVEOK);

        // Then
        assertThat(commandResult.ok(), is(true));
        assertThat((String) commandResult.get("serverUsed"), not(containsString(getPrimaryAsString())));
    }

    @Test
    public void shouldRunStringCommandAgainstSecondaryWhenSecondaryReadPreferenceSpecified() throws UnknownHostException {
        // Given
        assumeTrue(isReplicaSet(cleanupMongo));
        DB database = getReplicaSetDB();

        // When
        CommandResult commandResult = database.command("dbstats", secondary());

        // Then
        assertThat(commandResult.ok(), is(true));
        assertThat((String) commandResult.get("serverUsed"), not(containsString(getPrimaryAsString())));
    }

    @Test
    public void shouldRunCommandAgainstSecondaryWhenOnlySecondaryReadPreferenceSpecifiedAlongWithEncoder() throws UnknownHostException {
        // Given
        assumeTrue(isReplicaSet(cleanupMongo));
        DB database = getReplicaSetDB();

        // When
        CommandResult commandResult = database.command(new BasicDBObject("dbstats", 1), secondary(), DefaultDBEncoder.FACTORY.create());

        // Then
        assertThat(commandResult.ok(), is(true));
        assertThat((String) commandResult.get("serverUsed"), not(containsString(getPrimaryAsString())));
    }

    @Test
    public void shouldRunCommandAgainstPrimaryWhenOnlyPrimaryReadPreferenceSpecified() throws UnknownHostException {
        // Given
        assumeTrue(isReplicaSet(cleanupMongo));
        DB database = getReplicaSetDB();

        // When
        CommandResult commandResult = database.command(new BasicDBObject("dbstats", 1), primary());

        // Then
        assertThat(commandResult.ok(), is(true));
        assertThat((String) commandResult.get("serverUsed"), containsString(getPrimaryAsString()));
    }

    @Test
    public void shouldOverrideDefaultReadPreferenceWhenCommandReadPreferenceSpecified() throws UnknownHostException {
        // Given
        assumeTrue(isReplicaSet(cleanupMongo));
        DB database = getReplicaSetDB();
        database.setReadPreference(secondary());

        // When
        CommandResult commandResult = database.command(new BasicDBObject("dbstats", 1), primary());

        // Then
        assertThat(commandResult.ok(), is(true));
        assertThat((String) commandResult.get("serverUsed"), containsString(getPrimaryAsString()));
    }

    @Test
    public void shouldNotThrowAnExceptionwhenRequestDoneIsCalledWithoutFirstCallingRequestStart() throws UnknownHostException {
        getDatabase().requestDone();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowAnExceptionWhenDBNameContainsSpaces() {
        cleanupMongo.getDB("foo bar");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowAnExceptionWhenDBNameIsEmpty() {
        cleanupMongo.getDB("");
    }

    @Test
    public void shouldGetDefaultWriteConcern() {
        assertEquals(WriteConcern.ACKNOWLEDGED, getDatabase().getWriteConcern());
    }

    @Test
    public void shouldGetDefaultReadPreference() {
        assertEquals(ReadPreference.primary(), getDatabase().getReadPreference());
    }

    @Test
    public void shouldReturnCachedCollectionObjectIfExists() {
        DBCollection collection1 = getDatabase().getCollection("test");
        DBCollection collection2 = getDatabase().getCollection("test");
        assertThat("Checking that references are equal", collection1, sameInstance(collection2));
    }

    @Test
    public void shouldDropItself() {
        // when
        String databaseName = "drop-test-" + System.nanoTime();
        DB db = getMongoClient().getDB(databaseName);
        db.createCollection("drop-collection", new BasicDBObject());

        // then
        assertThat(cleanupMongo.getDatabaseNames(), hasItem(databaseName));

        // when
        db.dropDatabase();

        // then
        assertThat(cleanupMongo.getDatabaseNames(), not(hasItem(databaseName)));
    }

    @Test
    public void shouldGetCollectionNames() {
        getDatabase().dropDatabase();

        String[] collectionNames = {"c1", "c2", "c3"};

        for (final String name : collectionNames) {
            getDatabase().createCollection(name, new BasicDBObject());
        }

        assertThat(getDatabase().getCollectionNames(), hasItems(collectionNames));
    }

    @Test(expected = MongoException.class)
    public void shouldReceiveAnErrorIfCreatingCappedCollectionWithoutSize() {
        getDatabase().createCollection("someName", new BasicDBObject("capped", true));
    }

    @Test
    public void shouldCreateCappedCollection() {
        collection.drop();
        String collectionName = collection.getName();
        getDatabase().createCollection(collection.getName(), new BasicDBObject("capped", true)
                                                             .append("size", 242880));
        assertTrue(getDatabase().getCollection(collectionName).isCapped());
    }

    @Test
    public void shouldCreateCappedCollectionWithMaxNumberOfDocuments() {
        collection.drop();
        String collectionName = collection.getName();
        DBCollection cappedCollectionWithMax = getDatabase().createCollection(collectionName, new BasicDBObject("capped", true)
                                                                                              .append("size", 242880)
                                                                                              .append("max", 10));

//        assertThat((Boolean) cappedCollectionWithMax.getStats().get("capped"), is(true));
        assertThat((Integer) cappedCollectionWithMax.getStats().get("max"), is(10));

        for (int i = 0; i < 11; i++) {
            cappedCollectionWithMax.insert(new BasicDBObject("x", i));
        }
        assertThat(cappedCollectionWithMax.find().count(), is(10));

    }

    @Test(expected = CommandFailureException.class)
    public void shouldThrowErrorIfCreatingACappedCollectionWithANegativeSize() {
        collection.drop();
        DBObject creationOptions = BasicDBObjectBuilder.start().add("capped", true)
                                                       .add("size", -20).get();
        getDatabase().createCollection(collection.getName(), creationOptions);
    }

    @Test
    public void shouldCreateUncappedCollection() {
        collection.drop();
        String collectionName = collection.getName();

        BasicDBObject creationOptions = new BasicDBObject("capped", false);
        getDatabase().createCollection(collectionName, creationOptions);

        assertFalse(getDatabase().getCollection(collectionName).isCapped());
    }

    @Test
    @SuppressWarnings("deprecation")
    public void shouldDoEval() {
        assumeFalse(isAuthenticated());
        String code = "function(name, incAmount) {\n"
                      + "var doc = db.myCollection.findOne( { name : name } );\n"
                      + "doc = doc || { name : name , num : 0 , total : 0 , avg : 0 , _id: 1 };\n"
                      + "doc.num++;\n"
                      + "doc.total += incAmount;\n"
                      + "doc.avg = doc.total / doc.num;\n"
                      + "db.myCollection.save( doc );\n"
                      + "return doc;\n"
                      + "}";
        getDatabase().doEval(code, "eliot", 5);
        assertEquals(getDatabase().getCollection("myCollection").findOne(), new BasicDBObject("_id", 1.0)
                                                                            .append("avg", 5.0)
                                                                            .append("num", 1.0)
                                                                            .append("name", "eliot")
                                                                            .append("total", 5.0));
    }

    @Test(expected = MongoException.class)
    public void shouldThrowErrorwhileDoingEval() {
        assumeFalse(isAuthenticated());
        String code = "function(a, b) {\n"
                      + "var doc = db.myCollection.findOne( { name : b } );\n"
                      + "}";
        getDatabase().eval(code, 1);
    }

    @Test
    public void shouldGetStats() {
        assumeFalse(isSharded(getMongoClient()));
        assertThat(getDatabase().getStats(), hasFields(new String[]{"collections", "avgObjSize",
                                                                    "indexes", "db", "indexSize", "storageSize"}));
    }

    @Test
    public void shouldExecuteCommand() {
        CommandResult commandResult = getDatabase().command(new BasicDBObject("isMaster", 1));
        assertThat(commandResult, hasFields(new String[]{"ismaster", "maxBsonObjectSize", "ok", "serverUsed"}));
    }

    @Test
    public void shouldNotThrowAnExceptionOnCommandFailure() {
        CommandResult commandResult = getDatabase().command(new BasicDBObject("collStats", "a" + System.currentTimeMillis()));
        assertThat(commandResult, hasFields(new String[]{"serverUsed", "ok", "errmsg"}));
    }

    @Test
    public void shouldAddReadOnlyUser() {
        String userName = "newUser";
        String pwd = "pwd";
        getDatabase().addUser(userName, pwd.toCharArray(), true);
        try {
            assertCorrectUserExists(userName, pwd, true, getDatabase());
        } finally {
            getDatabase().removeUser(userName);
        }
    }

    @Test
    public void shouldAddReadWriteUser() {
        String userName = "newUser";
        String pwd = "pwd";
        getDatabase().addUser(userName, pwd.toCharArray(), false);
        try {
            assertCorrectUserExists(userName, pwd, false, getDatabase());
        } finally {
            getDatabase().removeUser(userName);
        }
    }

    @Test
    public void shouldAddReadWriteAdminUser() throws UnknownHostException {
        String userName = "newUser";
        String pwd = "pwd";
        MongoClient mongoClient = new MongoClient(getMongoClientURI());
        DB database = mongoClient.getDB(getDatabase().getName());
        database.addUser(userName, pwd.toCharArray(), false);
        try {
            assertTrue(database.authenticate(userName, pwd.toCharArray()));
            assertCorrectUserExists(userName, pwd, false, database);
        } finally {
            database.removeUser(userName);
            mongoClient.close();
        }
    }


    @Test
    public void shouldAddReadOnlyAdminUser() throws UnknownHostException {
        String readWriteUserName = "newUserReadWrite";
        String readOnlyUserName = "newUser";
        String pwd = "pwd";
        MongoClient mongoClient = new MongoClient(getMongoClientURI());
        DB database = mongoClient.getDB(getDatabase().getName());
        database.addUser(readWriteUserName, pwd.toCharArray(), false);
        database.authenticate(readWriteUserName, pwd.toCharArray());
        database.addUser(readOnlyUserName, pwd.toCharArray(), true);
        try {
            assertCorrectUserExists(readOnlyUserName, pwd, true, database);
        } finally {
            database.removeUser(readOnlyUserName);
            database.removeUser(readWriteUserName);
            mongoClient.close();
        }
    }

    @Test
    public void shouldRemoveUser() {
        String userName = "newUser";
        getDatabase().addUser(userName, "pwd".toCharArray(), true);
        getDatabase().removeUser(userName);
        assertThatUserIsRemoved(userName, getDatabase());
    }

    private void assertThatUserIsRemoved(final String userName, final DB database) {
        if (serverIsAtLeastVersion(2.6)) {
            CommandResult usersInfo = database.command(new BasicDBObject("usersInfo", userName));
            assertEquals(0, ((List) usersInfo.get("users")).size());
        }
        else {
            assertNull(database.getCollection("system.users").findOne(new BasicDBObject("user", userName)));
        }
    }


    private void assertCorrectUserExists(final String userName, final String password, final boolean isReadOnly, final DB database) {
        if (serverIsAtLeastVersion(2.6, database.getMongo())) {
            CommandResult usersInfo = database.command(new BasicDBObject("usersInfo", userName));
            DBObject user = (DBObject) ((List) usersInfo.get("users")).get(0);
            assertEquals(userName, user.get("user"));
            assertEquals(database.getName(), user.get("db"));
            assertEquals(getExpectedRole(isReadOnly, database), ((DBObject) ((List) user.get("roles")).get(0)).get("role"));
        }
        else {
            assertEquals(new BasicDBObject("user", userName).append("readOnly", isReadOnly)
                                                            .append("pwd", getDatabase()._hash(userName, password.toCharArray())),
                         database.getCollection("system.users").findOne(new BasicDBObject("user", userName),
                                                                             new BasicDBObject("_id", 0)));
        }
    }

    private String getExpectedRole(final boolean isReadOnly, final DB database) {
        if (database.getName().equals("admin")) {
           return isReadOnly ? "readAnyDatabase" : "root";
        } else {
           return isReadOnly ? "read" : "dbOwner";
        }
    }


    private DB getReplicaSetDB() throws UnknownHostException {
        Mongo mongo = new MongoClient(getMongoClientURI());
        return mongo.getDB("database-" + System.nanoTime());
    }

    private static Matcher<DBObject> hasFields(final String[] fields) {
        return new TypeSafeMatcher<DBObject>() {
            @Override
            protected boolean matchesSafely(final DBObject item) {
                for (final String fieldName : fields) {
                    if (!item.containsField(fieldName)) {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public void describeTo(final Description description) {
                description.appendText(" has fields ")
                           .appendValue(fields);
            }
        };
    }
}
