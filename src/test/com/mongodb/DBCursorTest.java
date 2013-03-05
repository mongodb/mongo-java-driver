/**
 *      Copyright (C) 2008 10gen Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.mongodb;

import com.mongodb.util.TestCase;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class DBCursorTest extends TestCase {

    public DBCursorTest() throws IOException , MongoException {
        super();
        cleanupDB = "com_mongodb_unittest_DBCursorTest";
        _db = cleanupMongo.getDB(cleanupDB);
    }

    @Test(groups = {"basic"})
    public void testGetServerAddressLoop() {

        final DBCollection c = _db.getCollection("getServerAddress");
        c.drop();

        // Insert some data.
        for (int i=0; i < 10; i++) c.insert(new BasicDBObject("one", "two"));

        final DBCursor cur = c.find();

        while (cur.hasNext()) {
            cur.next();
            assertNotNull(cur.getServerAddress());
        }
    }

    @Test(groups = {"basic"})
    public void testGetServerAddressQuery() {

        final DBCollection c = _db.getCollection("getServerAddress");
        c.drop();

        // Insert some data.
        for (int i=0; i < 10; i++) c.insert(new BasicDBObject("one", "two"));

        final DBCursor cur = c.find();
        cur.hasNext();
        assertNotNull(cur.getServerAddress());
    }

    @Test(groups = {"basic"})
    public void testGetServerAddressQuery1() {

        final DBCollection c = _db.getCollection("getServerAddress");
        c.drop();

        // Insert some data.
        for (int i=0; i < 10; i++) c.insert(new BasicDBObject("one", i));

        final DBCursor cur = c.find(new BasicDBObject("one", 9));
        cur.hasNext();
        assertNotNull(cur.getServerAddress());
    }

    @Test(groups = {"basic"})
    public void testCount() {
        try {
            DBCollection c = _db.getCollection("test");
            c.drop();

            assertEquals(c.find().count(), 0);

            BasicDBObject obj = new BasicDBObject();
            obj.put("x", "foo");
            c.insert(obj);

            assertEquals(c.find().count(), 1);
        }
        catch (MongoException e) {
            assertTrue(false);
        }
    }

    @Test(groups = {"basic"})
    public void testSnapshot() {
        DBCollection c = _db.getCollection("snapshot1");
        c.drop();
        for ( int i=0; i<100; i++ )
            c.save( new BasicDBObject( "x" , i ) );
        assertEquals( 100 , c.find().count() );
        assertEquals( 100 , c.find().toArray().size() );
        assertEquals( 100 , c.find().snapshot().count() );
        assertEquals( 100 , c.find().snapshot().toArray().size() );
        assertEquals( 100 , c.find().snapshot().limit(50).count() );
        assertEquals( 50 , c.find().snapshot().limit(50).toArray().size() );
    }

    @Test(groups = {"basic"})
    public void testOptions() {
        DBCollection c = _db.getCollection("test");
        DBCursor dbCursor = c.find();

        assertEquals(0, dbCursor.getOptions());
        dbCursor.setOptions( Bytes.QUERYOPTION_TAILABLE );
        assertEquals(Bytes.QUERYOPTION_TAILABLE, dbCursor.getOptions());
        dbCursor.addOption( Bytes.QUERYOPTION_SLAVEOK );
        assertEquals(Bytes.QUERYOPTION_TAILABLE | Bytes.QUERYOPTION_SLAVEOK, dbCursor.getOptions());
        dbCursor.resetOptions();
        assertEquals(0, dbCursor.getOptions());
    }

//    @Test
//    public void testTailable() {
//        DBCollection c = _db.createCollection( "tailableTest", new BasicDBObject( "capped", true ).append( "size", 10000));
//        DBCursor cursor = c.find( ).addOption( Bytes.QUERYOPTION_TAILABLE );
//
//        long start = System.currentTimeMillis();
//        System.err.println( "[ " + start + " ] Has Next?" +  cursor.hasNext());
//        cursor.next();
//        long end = System.currentTimeMillis();
//        System.err.println(  "[ " + end + " ] Tailable next returned." );
//        assertLess(start - end, 100);
//        c.drop();
//    }


    @Test//(enabled = false)
    public void testTailable() {
        DBCollection c = _db.getCollection("tail1");
        c.drop();
        _db.createCollection("tail1", new BasicDBObject("capped", true).append("size", 10000));
        for (int i = 0; i < 10; i++) {
            c.save(new BasicDBObject("x", i));
        }

        DBCursor cur = c.find().sort(new BasicDBObject("$natural", 1)).addOption(Bytes.QUERYOPTION_TAILABLE);

        while (cur.hasNext()) {
            cur.next();
            //do nothing...
        }

        assert (!cur.hasNext());
        c.save(new BasicDBObject("x", 12));
        assert (cur.hasNext());
        assertNotNull(cur.next());
        assert (!cur.hasNext());
    }

    @Test//(enabled = false)
    public void testTailableAwait() throws ExecutionException, TimeoutException, InterruptedException {
        DBCollection c = _db.getCollection("tail1");
        c.drop();
        _db.createCollection("tail1", new BasicDBObject("capped", true).append("size", 10000));
        for (int i = 0; i < 10; i++) {
            c.save(new BasicDBObject("x", i), WriteConcern.SAFE);
        }

        final DBCursor cur = c.find().sort(new BasicDBObject("$natural", 1)).addOption(Bytes.QUERYOPTION_TAILABLE | Bytes.QUERYOPTION_AWAITDATA);
        Callable<Object> callable = new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                try {
                    // the following call will block on the last hasNext
                    int i = 0;
                    while (cur.hasNext()) {
                        DBObject obj = cur.next();
                        i++;
                        if (i > 10)
                            return obj.get("x");
                    }

                    return null;
                } catch (Throwable e) {
                    return e;
                }
            }
        };

        ExecutorService es = Executors.newSingleThreadExecutor();
        Future<Object> future = es.submit(callable);

        Thread.sleep(5000);
        assertTrue(!future.isDone());

        // this doc should unblock thread
        c.save(new BasicDBObject("x", 10), WriteConcern.SAFE);
        Object retVal = future.get(5, TimeUnit.SECONDS);
        assertEquals(10, retVal);
    }
    
    @Test
    public void testBig(){
        DBCollection c = _db.getCollection("big1");
        c.drop();

        String bigString;
        {
            StringBuilder buf = new StringBuilder( 16000 );
            for ( int i=0; i<16000; i++ )
                buf.append( "x" );
            bigString = buf.toString();
        }

        int numToInsert = ( 15 * 1024 * 1024 ) / bigString.length();

        for ( int i=0; i<numToInsert; i++ )
            c.save( BasicDBObjectBuilder.start().add( "x" , i ).add( "s" , bigString ).get() );

        assert( 800 < numToInsert );

        assertEquals( numToInsert , c.find().count() );
        assertEquals( numToInsert , c.find().toArray().size() );
        assertEquals( numToInsert , c.find().limit(800).count() );
        assertEquals( 800 , c.find().limit(800).toArray().size() );

        // negative limit works like negative batchsize, for legacy reason
        int x = c.find().limit(-800).toArray().size();
        assertLess( x , 800 );

        DBCursor a = c.find();
        assertEquals( numToInsert , a.itcount() );

        DBCursor b = c.find().batchSize( 10 );
        assertEquals( numToInsert , b.itcount() );
        assertEquals( 10 , b.getSizes().get(0).intValue() );

        assertLess( a.numGetMores() , b.numGetMores() );

        assertEquals( numToInsert , c.find().batchSize(2).itcount() );
        assertEquals( numToInsert , c.find().batchSize(1).itcount() );

        assertEquals( numToInsert , _count( c.find( null , null).skip(  0 ).batchSize( 5 ) ) );
        assertEquals( 5 , _count( c.find( null , null).skip(  0 ).batchSize( -5 ) ) );
    }

    @SuppressWarnings("unchecked")
	int _count( Iterator i ){
        int c = 0;
        while ( i.hasNext() ){
            i.next();
            c++;
        }
        return c;
    }

    @Test
    public void testExplain(){
        DBCollection c = _db.getCollection( "explain1" );
        c.drop();

        for ( int i=0; i<100; i++ )
            c.save( new BasicDBObject( "x" , i ) );

        DBObject q = BasicDBObjectBuilder.start().push( "x" ).add( "$gt" , 50 ).get();

        assertEquals( 49 , c.find( q ).count() );
        assertEquals( 49 , c.find( q ).itcount() );
        assertEquals( 49 , c.find( q ).toArray().size() );
        assertEquals( 49 , c.find( q ).itcount() );
        assertEquals( 20 , c.find( q ).limit(20).itcount() );
        assertEquals( 20 , c.find( q ).limit(-20).itcount() );

        c.ensureIndex( new BasicDBObject( "x" , 1 ) );

        assertEquals( 49 , c.find( q ).count() );
        assertEquals( 49 , c.find( q ).toArray().size() );
        assertEquals( 49 , c.find( q ).itcount() );
        assertEquals( 20 , c.find( q ).limit(20).itcount() );
        assertEquals( 20 , c.find( q ).limit(-20).itcount() );

        assertEquals( 49 , c.find( q ).explain().get("n") );

        assertEquals( 20 , c.find( q ).limit(20).explain().get("n") );
        assertEquals( 20 , c.find( q ).limit(-20).explain().get("n") );

    }

    @Test
    public void testBatchWithActiveCursor(){
        DBCollection c = _db.getCollection( "testBatchWithActiveCursor" );
        c.drop();

        for ( int i=0; i<100; i++ )
            c.save( new BasicDBObject( "x" , i ) );

        try {
	        DBCursor cursor = c.find().batchSize(2); // setting to 1, actually sets to 2 (why, oh why?)
	        cursor.next(); //creates real cursor on server.
	        cursor.next();
	        assertEquals(0, cursor.numGetMores());
	        cursor.next();
	        assertEquals(1, cursor.numGetMores());
	        cursor.next();
	        cursor.next();
	        assertEquals(2, cursor.numGetMores());
	        cursor.next();
	        cursor.next();
	        assertEquals(3, cursor.numGetMores());
	        cursor.batchSize(20);
	        cursor.next();
	        cursor.next();
	        cursor.next();
	        assertEquals(4, cursor.numGetMores());
        } catch (IllegalStateException e) {
        	assertNotNull(e); // there must be a better way to detect this.
        }
    }

    @Test
    public void testBatchWithLimit(){
        DBCollection c = _db.getCollection( "batchWithLimit1" );
        c.drop();

        for ( int i=0; i<100; i++ )
            c.save( new BasicDBObject( "x" , i ) );

        assertEquals( 50 , c.find().limit(50).itcount() );
        assertEquals( 50 , c.find().batchSize( 5 ).limit(50).itcount() );
    }

    @Test
    public void testLargeBatch(){
        DBCollection c = _db.getCollection( "largeBatch1" );
        c.drop();

        int total = 50000;
        int batch = 10000;
        for ( int i=0; i<total; i++ )
            c.save( new BasicDBObject( "x" , i ) );

        DBCursor cursor = c.find().batchSize(batch);
        assertEquals( total , cursor.itcount() );
        assertEquals( total/batch + 1, cursor.getSizes().size());
    }
    @Test
    public void testSpecial(){
        DBCollection c = _db.getCollection( "testSpecial" );
        c.insert( new BasicDBObject( "x" , 1 ) );
        c.insert( new BasicDBObject( "x" , 2 ) );
        c.insert( new BasicDBObject( "x" , 3 ) );
        c.ensureIndex( "x" );

        for ( DBObject o : c.find().sort( new BasicDBObject( "x" , 1 ) ).addSpecial( "$returnKey" , 1 ) )
            assertTrue( o.get("_id") == null );

        for ( DBObject o : c.find().sort( new BasicDBObject( "x" , 1 ) ) )
            assertTrue( o.get("_id") != null );

    }

    @Test
    public void testUpsert(){
        DBCollection c = _db.getCollection( "upsert1" );
        c.drop();

        c.update( new BasicDBObject( "page" , "/" ), new BasicDBObject( "$inc" , new BasicDBObject( "count" , 1 ) ), true, false );
        c.update( new BasicDBObject( "page" , "/" ), new BasicDBObject( "$inc" , new BasicDBObject( "count" , 1 ) ), true, false );

        assertEquals( 1, c.getCount() );
        assertEquals( 2, c.findOne().get( "count" ) );
    }

    @Test
    public void testLimitAndBatchSize() {
        DBCollection c = _db.getCollection( "LimitAndBatchSize" );
        c.drop();

        for ( int i=0; i<1000; i++ )
            c.save( new BasicDBObject( "x" , i ) );

        DBObject q = BasicDBObjectBuilder.start().push( "x" ).add( "$lt" , 200 ).get();

        DBCursor cur = c.find( q );
        assertEquals(0, cur.getCursorId());
        assertEquals( 200 , cur.itcount() );

        cur = c.find( q ).limit(50);
        assertEquals(0, cur.getCursorId());
        assertEquals( 50 , cur.itcount() );

        cur = c.find( q ).batchSize(50);
        assertEquals(0, cur.getCursorId());
        assertEquals( 200 , cur.itcount() );

        cur = c.find( q ).batchSize(100).limit(50);
        assertEquals(0, cur.getCursorId());
        assertEquals( 50 , cur.itcount() );

        cur = c.find( q ).batchSize(-40);
        assertEquals(0, cur.getCursorId());
        assertEquals( 40 , cur.itcount() );

        cur = c.find( q ).limit(-100);
        assertEquals(0, cur.getCursorId());
        assertEquals( 100 , cur.itcount() );

        cur = c.find( q ).batchSize(-40).limit(20);
        assertEquals(0, cur.getCursorId());
        assertEquals( 20 , cur.itcount() );

        cur = c.find( q ).batchSize(-20).limit(100);
        assertEquals(0, cur.getCursorId());
        assertEquals( 20 , cur.itcount() );
    }

    @Test
    public void testSort(){
        DBCollection c = _db.getCollection( "SortTest" );
        c.drop();

        for ( int i=0; i<1000; i++ )
            c.save( new BasicDBObject( "x" , i ).append("y", 1000-i));

        //x ascending
        DBCursor cur = c.find().sort( new BasicDBObject("x", 1));
        int curmax = -100;
        while(cur.hasNext()){
        	int val = (Integer)cur.next().get("x");
        	assertTrue( val > curmax);
        	curmax = val;
        }
        
        //x desc
        cur = c.find().sort( new BasicDBObject("x", -1));
        curmax = 9999;
        while(cur.hasNext()){
        	int val = (Integer)cur.next().get("x");
        	assertTrue( val < curmax);
        	curmax = val;
        }
        
        //query and sort
        cur = c.find( QueryBuilder.start("x").greaterThanEquals(500).get()).sort(new BasicDBObject("y", 1));
        assertEquals(500, cur.count());
        curmax = -100;
        while(cur.hasNext()){
        	int val = (Integer)cur.next().get("y");
        	assertTrue( val > curmax);
        	curmax = val;
        }
    }

    @Test(expectedExceptions = NoSuchElementException.class)
    public void testShouldThrowNoSuchElementException() {
        DBCollection c = _db.getCollection("emptyCollection");

        DBCursor cursor = c.find();

        cursor.next();
    }

    @Test
    public void testHasFinalizer() throws UnknownHostException {
        DBCollection c = _db.getCollection( "HasFinalizerTest" );
        c.drop();

        for ( int i=0; i<1000; i++ )
            c.save( new BasicDBObject("_id", i), WriteConcern.SAFE);

        // finalizer is on by default so after calling hasNext should report that it has one
        DBCursor cursor = c.find();
        assertFalse(cursor.hasFinalizer());
        cursor.hasNext();
        assertTrue(cursor.hasFinalizer());
        cursor.close();

        // no finalizer if there is no cursor, as there should not be for a query with only one result
        cursor = c.find(new BasicDBObject("_id", 1));
        cursor.hasNext();
        assertFalse(cursor.hasFinalizer());
        cursor.close();

        // no finalizer if there is no cursor, as there should not be for a query with negative batch size
        cursor = c.find();
        cursor.batchSize(-1);
        cursor.hasNext();
        assertFalse(cursor.hasFinalizer());
        cursor.close();

        // finally, no finalizer if disabled in mongo options
        MongoClientOptions mongoOptions = new MongoClientOptions.Builder().cursorFinalizerEnabled(false).build();
        Mongo m = new MongoClient("127.0.0.1", mongoOptions);
        try {
            c = m.getDB(cleanupDB).getCollection("HasFinalizerTest");
            cursor = c.find();
            cursor.hasNext();
            assertFalse(cursor.hasFinalizer());
            cursor.close();
        } finally {
            m.close();
        }
    }
    
    final DB _db;

    public static void main( String args[] )
        throws Exception {
        (new DBCursorTest()).runConsole();

    }

}
