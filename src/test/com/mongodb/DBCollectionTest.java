/*
 * Copyright (c) 2008 - 2013 MongoDB Inc., Inc. <http://mongodb.com>
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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class DBCollectionTest extends TestCase {

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
        final Mongo mongo = new MongoClient( "127.0.0.1" );
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

        c.ensureIndex( new BasicDBObject( "x" , 1 ) );
        assertEquals( 2 , c.getIndexInfo().size() );

        c.ensureIndex( new BasicDBObject( "y" , 1 ) );
        assertEquals( 3 , c.getIndexInfo().size() );

        c.dropIndex( new BasicDBObject( "x" , 1 ) );
        assertEquals( 2 , c.getIndexInfo().size() );

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
    public void testEnsureIndex(){
        DBCollection c = collection;

        c.save( new BasicDBObject( "x" , 1 ) );
        assertEquals( 1 , c.getIndexInfo().size() );

        c.ensureIndex( new BasicDBObject( "x" , 1 ) , new BasicDBObject( "unique" , true ) );
        assertEquals( 2 , c.getIndexInfo().size() );
        assertEquals( Boolean.TRUE , c.getIndexInfo().get(1).get( "unique" ) );
    }

    @Test
    public void testEnsureNestedIndex(){
        DBCollection c = collection;

        BasicDBObject newDoc = new BasicDBObject( "x", new BasicDBObject( "y", 1 ) );
        c.save( newDoc );

        assertEquals( 1 , c.getIndexInfo().size() );
        c.ensureIndex( new BasicDBObject("x.y", 1), "nestedIdx1", false);
        assertEquals( 2 , c.getIndexInfo().size() );
    }


    @Test
    public void testIndexExceptions(){
        collection.insert(new BasicDBObject("x", 1));
        collection.insert(new BasicDBObject("x", 1));

        collection.ensureIndex(new BasicDBObject("y", 1));
        collection.resetIndexCache();
        collection.ensureIndex(new BasicDBObject("y", 1)); // make sure this doesn't throw
        collection.resetIndexCache();

        Exception failed = null;
        try {
            collection.ensureIndex(new BasicDBObject("x", 1), new BasicDBObject("unique", true));
        }
        catch ( MongoException.DuplicateKey e ){
            failed = e;
        }
        assertNotNull( failed );

    }

    @Test
    public void testMultiInsertNoContinue() {
        collection.setWriteConcern(WriteConcern.NORMAL);

        try {
            DBObject obj = collection.findOne();
            assertEquals(obj, null);

            ObjectId id = new ObjectId();
            DBObject inserted1 = BasicDBObjectBuilder.start("_id", id).add("x",1).add("y",2).get();
            DBObject inserted2 = BasicDBObjectBuilder.start("_id", id).add("x",3).add("y",4).get();
            DBObject inserted3 = BasicDBObjectBuilder.start().add("x",5).add("y",6).get();
            WriteResult r = collection.insert(inserted1, inserted2, inserted3);
            assertEquals(1, collection.count());
            assertFalse(collection.getWriteConcern().getContinueOnErrorForInsert());

            assertEquals(collection.count(), 1);
        } finally {
            collection.setWriteConcern(WriteConcern.ACKNOWLEDGED);
        }
    }

    @Test
    public void testMultiInsertWithContinue() {
        if (!serverIsAtLeastVersion(2.0)) {
            return;
        }
    
        DBObject obj = collection.findOne();
        assertEquals(obj, null);

        ObjectId id = new ObjectId();
        DBObject inserted1 = BasicDBObjectBuilder.start("_id", id).add("x",1).add("y",2).get();
        DBObject inserted2 = BasicDBObjectBuilder.start("_id", id).add("x",3).add("y",4).get();
        DBObject inserted3 = BasicDBObjectBuilder.start().add("x",5).add("y",6).get();
        WriteConcern newWC = WriteConcern.SAFE.continueOnError(true);
        try {
            collection.insert(newWC, inserted1, inserted2, inserted3);
            fail("Insert should have failed");
        } catch (MongoException e) {
            assertEquals(11000, e.getCode());
        }
        assertEquals( collection.count(), 2 );
    }

    @Test( expected =  IllegalArgumentException.class )
    public void testDotKeysFail() {
        DBObject obj = BasicDBObjectBuilder.start().add("x",1).add("y",2).add("foo.bar","baz").get();
        collection.insert(obj);
    }

    @Test( expected =  IllegalArgumentException.class )
    public void testNullKeysFail() {
        DBObject obj = BasicDBObjectBuilder.start().add("x",1).add("y",2).add("foo\0bar","baz").get();
        collection.insert(obj);
    }

    @Test( expected =  IllegalArgumentException.class )
    public void testNullKeysFailWhenNested() {
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
}
