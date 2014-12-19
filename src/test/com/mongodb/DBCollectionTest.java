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
import org.bson.types.ObjectId;
import org.junit.Test;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

public class DBCollectionTest extends TestCase {

    @Test
    public void shouldCreateIdOnInsertIfThereIsNone() {
        BasicDBObject document = new BasicDBObject();
        collection.insert(document);
        assertEquals(ObjectId.class, document.get("_id").getClass());
        assertEquals(document, collection.findOne());
    }

    @Test
    public void shouldCreateIdOnInsertIfTheValueIsNull() {
        BasicDBObject document = new BasicDBObject("_id", null);
        collection.insert(document);
        assertEquals(ObjectId.class, document.get("_id").getClass());
        assertEquals(document, collection.findOne());
    }

    @Test
    public void saveShouldInsertADocumentWithNullId() {
        BasicDBObject document = new BasicDBObject("_id", null);
        collection.save(document);
        assertEquals(ObjectId.class, document.get("_id").getClass());
        assertEquals(document, collection.findOne());
    }

    @Test
    public void saveShouldInsertADocumentWithNewObjectId() {
        ObjectId newObjectId = new ObjectId();
        BasicDBObject document = new BasicDBObject("_id", newObjectId);
        collection.save(document);
        assertEquals(newObjectId, document.get("_id"));
        assertEquals(document, collection.findOne());
    }

    @Test
    public void testMultiInsert() {
        DBCollection c = collection;

        DBObject obj = c.findOne();
        assertEquals(obj, null);

        DBObject inserted1 = BasicDBObjectBuilder.start().add("x",1).add("y",2).get();
        DBObject inserted2 = BasicDBObjectBuilder.start().add("x",3).add("y",3).get();
        c.insert(inserted1,inserted2);
    }

    @Test
    public void testLargeMultiInsert() {
        List<DBObject> documents = new ArrayList<DBObject>();
        for (int i = 0; i < 1001; i++) {
            documents.add(new BasicDBObject());
        }

        collection.insert(documents);
        assertEquals(1001, collection.count());
    }

    @Test
    public void testCappedCollection() {
        String collectionName = "testCapped";
        int collectionSize = 1000;

        DBCollection c = getDatabase().getCollection(collectionName);
        c.drop();

        DBObject options = new BasicDBObject("capped", true);
        options.put("size", collectionSize);
        c = getDatabase().createCollection(collectionName, options);

        assertEquals(c.isCapped(), true);
    }

    @Test
    public void testDuplicateKeyException() {
        DBCollection c = collection;

        DBObject obj = new BasicDBObject();
        c.insert(obj, WriteConcern.SAFE);
        try {
           c.insert(obj, WriteConcern.SAFE);
           fail();
        }
        catch (MongoException.DuplicateKey e) {
            assertNotNull(e.getCommandResult());
            assertEquals(11000, e.getCode());
        }
    }

    @Test
    public void testFindOne() {
        DBCollection c = collection;

        DBObject obj = c.findOne();
        assertEquals(obj, null);

        obj = c.findOne();
        assertEquals(obj, null);

        obj = c.findOne();
        assertEquals(obj, null);

        // Test that findOne works when fields is specified but no match is found
        // *** This is a Regression test for JAVA-411 ***
        obj = c.findOne(null, new BasicDBObject("_id", true));

        assertEquals(obj, null);

        DBObject inserted = BasicDBObjectBuilder.start().add("x",1).add("y",2).get();
        c.insert(inserted);
        c.insert(BasicDBObjectBuilder.start().add("_id", 123).add("x",2).add("z",2).get());

        obj = c.findOne(123);
        assertEquals(obj.get("_id"), 123);
        assertEquals(obj.get("x"), 2);
        assertEquals(obj.get("z"), 2);

        obj = c.findOne(123, new BasicDBObject("x", 1));
        assertEquals(obj.get("_id"), 123);
        assertEquals(obj.get("x"), 2);
        assertEquals(obj.containsField("z"), false);

        obj = c.findOne(new BasicDBObject("x", 1));
        assertEquals(obj.get("x"), 1);
        assertEquals(obj.get("y"), 2);

        obj = c.findOne(new BasicDBObject("x", 1), new BasicDBObject("y", 1));
        assertEquals(obj.containsField("x"), false);
        assertEquals(obj.get("y"), 2);
    }

    @Test
    public void testFindOneSort(){

    	DBCollection c = collection;

        DBObject obj = c.findOne();
        assertEquals(obj, null);

        c.insert(BasicDBObjectBuilder.start().add("_id", 1).add("x", 100).add("y", "abc").get());
        c.insert(BasicDBObjectBuilder.start().add("_id", 2).add("x", 200).add("y", "abc").get()); //max x
        c.insert(BasicDBObjectBuilder.start().add("_id", 3).add("x", 1).add("y", "abc").get());
        c.insert(BasicDBObjectBuilder.start().add("_id", 4).add("x", -100).add("y", "xyz").get()); //min x
        c.insert(BasicDBObjectBuilder.start().add("_id", 5).add("x", -50).add("y", "zzz").get());  //max y
        c.insert(BasicDBObjectBuilder.start().add("_id", 6).add("x", 9).add("y", "aaa").get());  //min y
        c.insert(BasicDBObjectBuilder.start().add("_id", 7).add("x", 1).add("y", "bbb").get());

        //only sort
        obj = c.findOne(new BasicDBObject(), null, new BasicDBObject("x", 1) );
        assertNotNull(obj);
        assertEquals(4, obj.get("_id"));

        obj = c.findOne(new BasicDBObject(), null, new BasicDBObject("x", -1));
        assertNotNull(obj);
        assertEquals(obj.get("_id"), 2);

        //query and sort
        obj = c.findOne(new BasicDBObject("x", 1), null, BasicDBObjectBuilder.start().add("x", 1).add("y", 1).get() );
        assertNotNull(obj);
        assertEquals(obj.get("_id"), 3);

        obj = c.findOne( QueryBuilder.start("x").lessThan(2).get(), null, BasicDBObjectBuilder.start().add("y", -1).get() );
        assertNotNull(obj);
        assertEquals(obj.get("_id"), 5);

    }

    /**
     * This was broken recently. Adding test.
     */
    @Test
    public void testDropDatabase() throws Exception {
        final Mongo mongo = new MongoClient(getMongoClientURI());
        mongo.getDB("com_mongodb_unittest_dropDatabaseTest").dropDatabase();
        mongo.close();
    }

    @Test
    public void testDropIndex(){
        DBCollection c = collection;

        c.save( new BasicDBObject( "x" , 1 ) );
        assertEquals( 1 , c.getIndexInfo().size() );

        c.ensureIndex( new BasicDBObject( "x" , 1 ) );
        assertEquals( 2 , c.getIndexInfo().size() );

        c.dropIndexes();
        assertEquals( 1 , c.getIndexInfo().size() );

        c.createIndex( new BasicDBObject( "x" , 1 ) );
        assertEquals( 2 , c.getIndexInfo().size() );

        c.dropIndexes();
        assertEquals( 1 , c.getIndexInfo().size() );

        c.ensureIndex( new BasicDBObject( "x" , 1 ) );
        assertEquals( 2 , c.getIndexInfo().size() );

        c.ensureIndex( new BasicDBObject( "y" , 1 ) );
        assertEquals( 3 , c.getIndexInfo().size() );

        c.dropIndex( new BasicDBObject( "x" , 1 ) );
        assertEquals( 2 , c.getIndexInfo().size() );

        c.dropIndexes();
        assertEquals( 1 , c.getIndexInfo().size() );

        c.createIndex( new BasicDBObject( "x" , 1 ) );
        assertEquals( 2 , c.getIndexInfo().size() );

        c.createIndex( new BasicDBObject( "y" , 1 ) );
        assertEquals( 3 , c.getIndexInfo().size() );

        c.dropIndex( new BasicDBObject( "x" , 1 ) );
        assertEquals( 2 , c.getIndexInfo().size() );

        // Now drop an index that no longer exists.  It should throw an exception
        try {
            c.dropIndex( new BasicDBObject( "x" , 1 ) );
            fail();
        } catch (CommandFailureException e) {
            // all good
        }

        // Now drop an index on a non-existing collection.  It should not throw an exception
        getDatabase().getCollection("does.not.exist").dropIndex(new BasicDBObject("x", 1));

        c.dropIndexes();
    }

    @Test
    public void testGetIndexInfo() {
        DBCollection c = collection;

        assertEquals(0, c.getIndexInfo().size());

        c.save( new BasicDBObject( "x" , 1 ) );
        assertEquals(1, c.getIndexInfo().size());
    }

    @Test
    public void testGetCollectionNames() {
        getDatabase().dropDatabase();
        assertTrue(getDatabase().getCollectionNames().isEmpty());

        collection.insert(new BasicDBObject());
        assertFalse(getDatabase().getCollectionNames().isEmpty());
        assertTrue(getDatabase().getCollectionNames().contains(collection.getName()));
    }

    @Test
    public void testDrop() {
        // ensure it exists
        collection.insert(new BasicDBObject());
        assertTrue(getDatabase().getCollectionNames().contains(collection.getName()));

        collection.drop();
        assertFalse(getDatabase().getCollectionNames().contains(collection.getName()));

        // Now drop a collection that no longer exists.  It should not throw
        collection.drop();
        assertFalse(getDatabase().getCollectionNames().contains(collection.getName()));
    }

    @Test
    public void testGenIndexName(){
        BasicDBObject o = new BasicDBObject();
        o.put( "x" , 1 );
        assertEquals("x_1", DBCollection.genIndexName(o));

        o.put( "x" , "1" );
        assertEquals("x_1", DBCollection.genIndexName(o));

        o.put( "x" , "2d" );
        assertEquals("x_2d", DBCollection.genIndexName(o));

        o.put( "y" , -1 );
        assertEquals("x_2d_y_-1", DBCollection.genIndexName(o));

        o.put( "x" , 1 );
        o.put( "y" , 1 );
        o.put( "a" , 1 );
        assertEquals( "x_1_y_1_a_1" , DBCollection.genIndexName(o) );

        o = new BasicDBObject();
        o.put( "z" , 1 );
        o.put( "a" , 1 );
        assertEquals( "z_1_a_1" , DBCollection.genIndexName(o) );
    }

    @Test
    public void testDistinct(){
        DBCollection c = collection;

        for ( int i=0; i<100; i++ ){
            BasicDBObject o = new BasicDBObject();
            o.put( "_id" , i );
            o.put( "x" , i % 10 );
            c.save( o );
        }

        List l = c.distinct( "x" );
        assertEquals( 10 , l.size() );

        l = c.distinct( "x" , new BasicDBObject( "_id" , new BasicDBObject( "$gt" , 95 ) ) );
        assertEquals( 4 , l.size() );

    }

    @Test
    public void testCreateIndexByName() {
        DBCollection c = collection;

        c.save(new BasicDBObject("x", 1));
        assertEquals(1, c.getIndexInfo().size());

        c.createIndex("x");
        assertEquals( 2 , c.getIndexInfo().size() );
        DBObject indexInfo = c.getIndexInfo().get(1);
        assertEquals("x_1", indexInfo.get("name"));
    }

    @Test
    public void testCreateIndexByKeys() {
        DBCollection c = collection;

        c.save(new BasicDBObject("x", 1));
        assertEquals(1, c.getIndexInfo().size());

        c.createIndex(new BasicDBObject("x", 1));
        assertEquals( 2 , c.getIndexInfo().size() );
        DBObject indexInfo = c.getIndexInfo().get(1);
        assertEquals("x_1", indexInfo.get("name"));
    }

    @Test
    public void testCreateIndexByKeysName() {
        DBCollection c = collection;

        c.save(new BasicDBObject("x", 1));
        assertEquals(1, c.getIndexInfo().size());

        c.createIndex(new BasicDBObject("x", 1), "zulu");
        assertEquals( 2 , c.getIndexInfo().size() );
        DBObject indexInfo = c.getIndexInfo().get(1);
        assertEquals("zulu", indexInfo.get("name"));
    }

    @Test
    public void testCreateIndexByKeysNameUnique() {
        DBCollection c = collection;

        c.save(new BasicDBObject("x", 1));
        assertEquals(1, c.getIndexInfo().size());

        c.createIndex(new BasicDBObject("x", 1), "zulu", true);
        assertEquals( 2 , c.getIndexInfo().size() );
        DBObject indexInfo = c.getIndexInfo().get(1);
        assertEquals("zulu", indexInfo.get("name"));
        assertTrue((Boolean) indexInfo.get("unique"));
    }

    @Test
    public void testEnsureIndex(){
        DBCollection c = collection;

        c.save( new BasicDBObject( "x" , 1 ) );
        assertEquals( 1 , c.getIndexInfo().size() );

        c.ensureIndex( new BasicDBObject( "x" , 1 ) , new BasicDBObject( "unique" , true ) );
        assertEquals( 2 , c.getIndexInfo().size() );
        DBObject indexInfo = c.getIndexInfo().get(1);
        assertEquals("x_1", indexInfo.get("name"));

        c.drop();

        c.createIndex(new BasicDBObject("x", 1), new BasicDBObject("unique", true));
        indexInfo = c.getIndexInfo().get(1);
        assertEquals( Boolean.TRUE , indexInfo.get("unique") );
        assertEquals("x_1", indexInfo.get("name"));
    }

    @Test
    public void testEnsureNestedIndex(){
        DBCollection c = collection;

        BasicDBObject newDoc = new BasicDBObject( "x", new BasicDBObject( "y", 1 ) );
        c.save( newDoc );

        assertEquals( 1 , c.getIndexInfo().size() );
        c.ensureIndex( new BasicDBObject("x.y", 1), "nestedIdx1", false);
        assertEquals( 2 , c.getIndexInfo().size() );
        assertEquals( "nestedIdx1" , c.getIndexInfo().get(1).get("name") );
        
        c.drop();
        c.createIndex(new BasicDBObject("x.y", 1), new BasicDBObject("name", "nestedIdx1").append("unique", false));
        assertEquals(2, c.getIndexInfo().size());
        assertEquals( "nestedIdx1" , c.getIndexInfo().get(1).get("name") );
    }


    @Test(expected = DuplicateKeyException.class)
    public void testEnsureIndexExceptions(){
        collection.insert(new BasicDBObject("x", 1));
        collection.insert(new BasicDBObject("x", 1));

        collection.ensureIndex(new BasicDBObject("y", 1));
        collection.resetIndexCache();
        try {
            collection.ensureIndex(new BasicDBObject("y", 1)); // make sure this doesn't throw
        } catch (Exception e) {
            fail("Trying to create an existing index should not fail.");            
        }
        collection.resetIndexCache();

        collection.ensureIndex(new BasicDBObject("x", 1), new BasicDBObject("unique", true));
    }

    @Test(expected = DuplicateKeyException.class)
    public void testCreateIndexExceptions(){
        collection.insert(new BasicDBObject("x", 1));
        collection.insert(new BasicDBObject("x", 1));

        collection.createIndex(new BasicDBObject("y", 1));
        try {
            collection.createIndex(new BasicDBObject("y", 1)); // make sure this doesn't throw
        } catch (Exception e) {
            fail("Trying to create an existing index should not fail.");
        }

        collection.createIndex(new BasicDBObject("x", 1), new BasicDBObject("unique", true));
    }

    @Test
    public void testMultiInsertNoContinue() {
        DBObject obj = collection.findOne();
        assertEquals(obj, null);

        List<DBObject> documents = Arrays.<DBObject>asList(new BasicDBObject("_id", 1).append("x", 1).append("y", 2),
                                                           new BasicDBObject("_id", 1).append("x", 3).append("y", 4),
                                                           new BasicDBObject("x", 5).append("y", 6));
        try {
            collection.insert(documents, WriteConcern.ACKNOWLEDGED);
            fail("Insert should have failed");
        } catch (MongoException e) {
            assertEquals(11000, e.getCode());
        }
        assertEquals(1, collection.count());

        try {
            collection.insert(documents, new InsertOptions());
            fail("Insert should have failed");
        } catch (MongoException e) {
            assertEquals(11000, e.getCode());
        }
        assertEquals(1, collection.count());
    }

    @Test
    public void testMultiInsertWithContinue() {
        if (!serverIsAtLeastVersion(2.0)) {
            return;
        }

        DBObject obj = collection.findOne();
        assertEquals(obj, null);

        List<DBObject> documents = Arrays.<DBObject>asList(new BasicDBObject("_id", 1).append("x", 1).append("y", 2),
                                                           new BasicDBObject("_id", 1).append("x", 3).append("y", 4),
                                                           new BasicDBObject("x", 5).append("y", 6));
        WriteConcern writeConcernWithContinueOnError = WriteConcern.SAFE.continueOnError(true);

        try {
            collection.insert(documents, writeConcernWithContinueOnError);
            fail("Insert should have failed");
        } catch (MongoException e) {
            assertEquals(11000, e.getCode());
        }
        assertEquals(collection.count(), 2);

        collection.drop();
        try {
            collection.insert(documents, new InsertOptions().writeConcern(WriteConcern.ACKNOWLEDGED).continueOnError(true));
            fail("Insert should have failed");
        } catch (MongoException e) {
            assertEquals(11000, e.getCode());
        }
        assertEquals(collection.count(), 2);

        collection.drop();
        try {
            collection.insert(documents, new InsertOptions().writeConcern(writeConcernWithContinueOnError));
            fail("Insert should have failed");
        } catch (MongoException e) {
            assertEquals(11000, e.getCode());
        }
        assertEquals(collection.count(), 2);
    }

    @Test( expected =  IllegalArgumentException.class )
    public void testDotKeysFail() {
        DBObject obj = BasicDBObjectBuilder.start().add("x",1).add("y",2).add("foo.bar","baz").get();
        collection.insert(obj);
    }

    @Test( expected = IllegalArgumentException.class )
    public void testNullCharacterInKeyFails() {
        DBObject obj = BasicDBObjectBuilder.start().add("x",1).add("y",2).add("foo\0bar","baz").get();
        collection.insert(obj);
    }

    @Test
    public void testNullCharacterInValueSucceeds() {
        DBObject obj = BasicDBObjectBuilder.start().add("x", "foo\0bar").add("nested", new BasicDBObject("y", "foo\0bar")).get();
        collection.insert(obj);
        assertEquals(obj, collection.findOne());
    }

    @Test( expected = IllegalArgumentException.class )
    public void testNullCharacterInNestedKeyFails() {
        final BasicDBList list = new BasicDBList();
        list.add(new BasicDBObject("foo\0bar","baz"));
        DBObject obj = BasicDBObjectBuilder.start().add("x", list).get();
        collection.insert(obj);
    }

    @Test
    public void testLazyDocKeysPass() {
        DBObject obj = BasicDBObjectBuilder.start().add("_id", "lazydottest1").add("x",1).add("y",2).add("foo.bar","baz").get();

        //convert to a lazydbobject
        DefaultDBEncoder encoder = new DefaultDBEncoder();
        byte[] encodedBytes = encoder.encode(obj);

        LazyDBDecoder lazyDecoder = new LazyDBDecoder();
        DBObject lazyObj = lazyDecoder.decode(encodedBytes, collection);

        collection.insert(lazyObj);

        DBObject insertedObj = collection.findOne();
        assertEquals("lazydottest1", insertedObj.get("_id"));
        assertEquals(1, insertedObj.get("x"));
        assertEquals(2, insertedObj.get("y"));
        assertEquals("baz", insertedObj.get("foo.bar"));
    }

    @Test
    public void testFindAndUpdateTimeout() {
        assumeFalse(isSharded(getMongoClient()));
        checkServerVersion(2.5);
        collection.insert(new BasicDBObject("_id", 1));
        enableMaxTimeFailPoint();
        try {
            collection.findAndModify(new BasicDBObject("_id", 1), null, null, false, new BasicDBObject("$set", new BasicDBObject("x", 1)),
                                     false, false, 1, TimeUnit.SECONDS);
            fail("Show have thrown");
        } catch (MongoExecutionTimeoutException e) {
            assertEquals(50, e.getCode());
        } finally {
            disableMaxTimeFailPoint();
        }
    }

    @Test
    public void testFindAndReplaceTimeout() {
        assumeFalse(isSharded(getMongoClient()));
        checkServerVersion(2.5);
        collection.insert(new BasicDBObject("_id", 1));
        enableMaxTimeFailPoint();
        try {
            collection.findAndModify(new BasicDBObject("_id", 1), null, null, false, new BasicDBObject("x", 1), false, false,
                                     1, TimeUnit.SECONDS);
            fail("Show have thrown");
        } catch (MongoExecutionTimeoutException e) {
            assertEquals(50, e.getCode());
        } finally {
            disableMaxTimeFailPoint();
        }
    }

    @Test
    public void testFindAndRemoveTimeout() {
        assumeFalse(isSharded(getMongoClient()));
        checkServerVersion(2.5);
        collection.insert(new BasicDBObject("_id", 1));
        enableMaxTimeFailPoint();
        try {
            collection.findAndModify(new BasicDBObject("_id", 1), null, null, true, null, false, false, 1, TimeUnit.SECONDS);
            fail("Show have thrown");
        } catch (MongoExecutionTimeoutException e) {
            assertEquals(50, e.getCode());
        } finally {
            disableMaxTimeFailPoint();
        }
    }

    @Test
    public void testFindAndReplaceA16MDocument() {
        BasicDBObject documentWithJustId = new BasicDBObject("_id", 42);
        DBObject foundDocument = collection.findAndModify(documentWithJustId, new BasicDBObject("_id", 1), null, false,
                                                          new BasicDBObject("_id", 42).append("b", new byte[16 * 1024 * 1024 - 30]), true,
                                                          true);
        assertEquals(documentWithJustId, foundDocument);
    }

    @Test
    public void testWriteConcernExceptionOnInsert() throws UnknownHostException {
        assumeTrue(isReplicaSet(getMongoClient()));
        MongoClient mongoClient = new MongoClient(getMongoClientURI());
        try {
            DBCollection localCollection = mongoClient.getDB(collection.getDB().getName()).getCollection(collection.getName());
            WriteResult writeResult = localCollection.insert(new BasicDBObject(), new WriteConcern(5, 1, false, false));
            fail("Expected update to error.  Instead, succeeded with: " + writeResult);
        } catch (WriteConcernException e) {
            assertNotNull(e.getServerAddress());
            assertNotNull(e.getErrorMessage());
            assertNotNull(e.getCommandResult().get("err"));
            assertEquals(0, e.getCommandResult().get("n"));
        } finally {
            mongoClient.close();
        }
    }

    @Test
    public void testWriteConcernExceptionOnUpdate() throws UnknownHostException {
        assumeTrue(isReplicaSet(getMongoClient()));
        MongoClient mongoClient = new MongoClient(getMongoClientURI());
        ObjectId id = new ObjectId();
        try {
            DBCollection localCollection = mongoClient.getDB(collection.getDB().getName()).getCollection(collection.getName());
            WriteResult writeResult = localCollection.update(new BasicDBObject("_id", id),
                                                             new BasicDBObject("$set", new BasicDBObject("x", 1)),
                                                             true, false,
                                                             new WriteConcern(5, 1, false, false));
            fail("Expected update to error.  Instead, succeeded with: " + writeResult);
        } catch (WriteConcernException e) {
            assertNotNull(e.getServerAddress());
            assertNotNull(e.getErrorMessage());
            assertNotNull(e.getCommandResult().get("err"));
            assertEquals(1, e.getCommandResult().get("n"));
            assertEquals(id, e.getCommandResult().get("upserted"));
        } finally {
            mongoClient.close();
        }
    }

    @Test
    public void testWriteConcernExceptionOnRemove() throws UnknownHostException {
        assumeTrue(isReplicaSet(getMongoClient()));
        MongoClient mongoClient = new MongoClient(getMongoClientURI());
        try {
            DBCollection localCollection = mongoClient.getDB(collection.getDB().getName()).getCollection(collection.getName());
            localCollection.insert(new BasicDBObject());
            WriteResult writeResult = localCollection.remove(new BasicDBObject(), new WriteConcern(5, 1, false, false));
            fail("Expected update to error.  Instead, succeeded with: " + writeResult);
        } catch (WriteConcernException e) {
            assertNotNull(e.getServerAddress());
            assertNotNull(e.getErrorMessage());
            assertNotNull(e.getCommandResult().get("err"));
            assertEquals(1, e.getCommandResult().get("n"));
        } finally {
            mongoClient.close();
        }
    }
    @Test
    public void testBulkWriteConcernException() throws UnknownHostException {
        assumeTrue(isReplicaSet(getMongoClient()));
        MongoClient mongoClient = new MongoClient(getMongoClientURI());
        try {
            DBCollection localCollection = mongoClient.getDB(collection.getDB().getName()).getCollection(collection.getName());
            BulkWriteOperation bulkWriteOperation = localCollection.initializeUnorderedBulkOperation();
            bulkWriteOperation.insert(new BasicDBObject());
            bulkWriteOperation.execute(new WriteConcern(5, 1, false, false));
            fail();
        } catch (BulkWriteException e) {
            assertNotNull(e.getWriteConcernError());  // unclear what else we can reliably assert here
        } finally {
            mongoClient.close();
        }
    }

    @Test
    public void testParallelScan() throws UnknownHostException {
        assumeTrue(serverIsAtLeastVersion(2.5));
        assumeFalse(isSharded(getMongoClient()));

        Set<Integer> ids = new HashSet<Integer>();

        for (int i = 0; i < 2000; i++) {
            ids.add(i);
            collection.insert(new BasicDBObject("_id", i));
        }

        int numCursors = 10;
        List<Cursor> cursors = collection.parallelScan(ParallelScanOptions.builder().numCursors(numCursors).batchSize(1000).build());
        assertTrue(cursors.size() <= numCursors);

        for (Cursor cursor : cursors) {
            while (cursor.hasNext()) {
                Integer id = (Integer) cursor.next().get("_id");
                assertTrue(ids.remove(id));
            }
        }

        assertTrue(ids.isEmpty());
    }
}
