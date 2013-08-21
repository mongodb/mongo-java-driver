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

package com.mongodb;

import category.ReplicaSet;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mongodb.Document;
import org.mongodb.connection.impl.NativeAuthenticationHelper;
import org.mongodb.operation.FindUserOperation;

import java.net.UnknownHostException;

import static com.mongodb.DBObjectMatchers.hasFields;
import static com.mongodb.DBObjectMatchers.hasSubdocument;
import static com.mongodb.Fixture.getMongoClient;
import static com.mongodb.Fixture.getOptions;
import static com.mongodb.Fixture.getPrimary;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mongodb.Fixture.getSession;

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
    public void shouldReturnCachedCollectionObjectIfExists() {
        final DBCollection collection1 = database.getCollection("test");
        final DBCollection collection2 = database.getCollection("test");
        assertThat("Checking that references are equal", collection1, sameInstance(collection2));
    }

    @Test
    public void shouldDropItself() {
        final String databaseName = "drop-test-" + System.nanoTime();
        final DB db = getMongoClient().getDB(databaseName);
        db.createCollection("tmp", new BasicDBObject());
        assertThat(getClient().getDatabaseNames(), hasItem(databaseName));
        db.dropDatabase();
        assertThat(getClient().getDatabaseNames(), not(hasItem(databaseName)));
    }

    @Test
    public void shouldGetCollectionNames() {
        database.dropDatabase();

        final String[] collectionNames = {"c1", "c2", "c3"};

        for (final String name : collectionNames) {
            database.createCollection(name, new BasicDBObject());
        }

        assertThat(database.getCollectionNames(), hasItems(collectionNames));
    }

    @Test(expected = MongoException.class)
    public void shouldReceiveAnErrorIfCreatingCappedCollectionWithoutSize() {
        database.createCollection("c1", new BasicDBObject("capped", true));
    }

    @Test
    public void shouldCreateCappedCollection() {
        database.createCollection("c1", new BasicDBObject("capped", true).append("size", 242880));
        assertTrue(database.getCollection("c1").isCapped());
    }

    @Test
    public void shouldCreateCollectionWithMax() {
        collection.drop();
        final DBCollection c1 = database.createCollection(collectionName, new BasicDBObject("capped", true)
                .append("size", 242880)
                .append("max", 10)
        );

        assertThat(c1.getStats(), hasSubdocument(new BasicDBObject("capped", true).append("max", 10)));
    }

    @Test(expected = MongoDuplicateKeyException.class)
    public void shouldGetDuplicateKeyException() {
        final DBObject doc = new BasicDBObject("_id", 1);
        collection.insert(doc);
        collection.insert(doc, WriteConcern.ACKNOWLEDGED);
    }

    @Test
    public void shouldDoEval() {
        final String code = "function(name, incAmount) {\n" +
                "var doc = db.myCollection.findOne( { name : name } );\n" +
                "doc = doc || { name : name , num : 0 , total : 0 , avg : 0 , _id: 1 };\n" +
                "doc.num++;\n" +
                "doc.total += incAmount;\n" +
                "doc.avg = doc.total / doc.num;\n" +
                "db.myCollection.save( doc );\n" +
                "return doc;\n" +
                "}";
        database.doEval(code, "eliot", 5);
        assertEquals(database.getCollection("myCollection").findOne(), new BasicDBObject("_id", 1.0)
                .append("avg", 5.0)
                .append("num", 1.0)
                .append("name", "eliot")
                .append("total", 5.0)
        );
    }

    @Test(expected = MongoException.class)
    @Ignore("Will be added as soon as error-handling mechanism settle down")
    public void shouldThrowErrorwhileDoingEval() {
        final String code = "function(a, b) {\n" +
                "var doc = db.myCollection.findOne( { name : b } );\n" +
                "}";
        database.eval(code, 1);
    }

    @Test
    public void shouldGetStats() {
        assertThat(database.getStats(), hasFields(new String[]{"collections", "avgObjSize", "indexes", "db", "indexSize", "storageSize"}));
    }

    @Test
    public void shouldExecuteCommand() {
        final CommandResult commandResult = database.command(new BasicDBObject("isMaster", 1));
        assertThat(commandResult, hasFields(new String[]{"ismaster", "maxBsonObjectSize", "ok", "serverUsed"}));
    }

    @Test
    @Ignore("Will be added after 'options' implementation")
    public void shouldExecuteCommandWithOptions() {
        database.command(new BasicDBObject("isMaster", 1), Bytes.QUERYOPTION_SLAVEOK);
        database.command(new BasicDBObject("isMaster", 1), Bytes.QUERYOPTION_SLAVEOK);
    }

    @Test
    @Category(ReplicaSet.class)
    public void shouldExecuteCommandWithReadPreference() {
        final CommandResult commandResult = database.command(new BasicDBObject("dbStats", 1).append("scale", 1), 0, ReadPreference.secondary());
        assertThat(commandResult, hasFields(new String[]{"collections", "avgObjSize", "indexes", "db", "indexSize", "storageSize"}));
    }

    @Test
    public void shouldNotThrowAnExceptionOnCommandFailure() {
        final CommandResult commandResult = database.command(new BasicDBObject("collStats", "a" + System.currentTimeMillis()));
        assertThat(commandResult, hasFields(new String[]{"serverUsed", "ok", "errmsg"}));
    }

    @Test
    public void shouldAddUser() {
        String userName = "jeff";
        char[] password = "123".toCharArray();
        boolean readOnly = true;
        database.addUser(userName, password, readOnly);
        Document userDocument = new FindUserOperation(database.getName(), database.getBufferPool(), userName, getSession(),
                true).execute();
        assertEquals(userName, userDocument.get("user"));
        assertEquals(NativeAuthenticationHelper.createAuthenticationHash(userName, password), userDocument.get("pwd"));
        assertEquals(readOnly, userDocument.get("readOnly"));
    }

    @Test
    public void shouldUpdateUser() {
        String userName = "jeff";

        char[] password = "123".toCharArray();
        boolean readOnly = true;
        database.addUser(userName, password, readOnly);

        char[] newPassword = "345".toCharArray();
        boolean newReadOnly = false;
        database.addUser(userName, newPassword, newReadOnly);

        Document userDocument = new FindUserOperation(database.getName(), database.getBufferPool(), userName, getSession(),
                true).execute();
        assertEquals(userName, userDocument.get("user"));
        assertEquals(NativeAuthenticationHelper.createAuthenticationHash(userName, newPassword), userDocument.get("pwd"));
        assertEquals(newReadOnly, userDocument.get("readOnly"));
    }

    @Test
    public void shouldRemoveUser() {
        String userName = "jeff";

        char[] password = "123".toCharArray();
        boolean readOnly = true;
        database.addUser(userName, password, readOnly);

        database.removeUser(userName);

        assertNull(new FindUserOperation(database.getName(), database.getBufferPool(), userName, getSession(),
                true).execute());
    }

    @Test
    public void whenRequestStartCallsAreNestedThenTheConnectionShouldBeReleaseOnLastCallToRequestEnd() throws UnknownHostException, InterruptedException {
        Mongo m = new MongoClient(getPrimary(),
                MongoClientOptions.builder()
                        .connectionsPerHost(1)
                        .maxWaitTime(10)
                        .socketFactory(getOptions().getSocketFactory())
                        .build()
        );
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
    public void whenRequestDoneIsCalledWithoutFirstCallingRequestStartNoExceptionIsThrown() throws UnknownHostException {
        database.requestDone();
    }
}
