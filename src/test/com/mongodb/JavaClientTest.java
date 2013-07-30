// JavaClientTest.java
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

import com.mongodb.util.JSON;
import com.mongodb.util.TestCase;
import com.mongodb.util.Util;
import org.bson.BSON;
import org.bson.Transformer;
import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.CodeWScope;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.testng.annotations.Test;

import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

public class JavaClientTest extends TestCase {

    public JavaClientTest() {
	_mongo = cleanupMongo;
	cleanupDB = "com_mongodb_unittest_JavaClientTest";
	_db = cleanupMongo.getDB( cleanupDB );
    }

    @Test
    public void test1()
        throws MongoException {
        DBCollection c = _db.getCollection( "test1" );
        c.drop();

        DBObject m = new BasicDBObject();
        m.put( "name" , "eliot" );
        m.put( "state" , "ny" );

        c.save( m );
        assert( m.containsField( "_id" ) );

        Map out = (Map)(c.findOne( m.get( "_id" )));
        assertEquals( "eliot" , out.get( "name" ) );
        assertEquals( "ny" , out.get( "state" ) );
    }

    @Test
    public void test2()
        throws MongoException {
        DBCollection c = _db.getCollection( "test2" );
        c.drop();

        DBObject m = new BasicDBObject();
        m.put( "name" , "eliot" );
        m.put( "state" , "ny" );

        Map<String,Object> sub = new HashMap<String,Object>();
        sub.put( "bar" , "1z" );
        m.put( "foo" , sub );

        c.save( m );

        assert( m.containsField( "_id" ) );

        Map out = (Map)(c.findOne( m.get( "_id" )));
        assertEquals( "eliot" , out.get( "name" ) );
        assertEquals( "ny" , out.get( "state" ) );

        Map z = (Map)out.get( "foo" );
        assertNotNull( z );
        assertEquals( "1z" , z.get( "bar" ) );
    }

    @Test
    public void testWhere1()
        throws MongoException {
        DBCollection c = _db.getCollection( "testWhere1" );
        c.drop();
        assertNull( c.findOne() );

        c.save( BasicDBObjectBuilder.start().add( "a" , 1 ).get() );
        assertNotNull( c.findOne() != null );

        assertNotNull( c.findOne( BasicDBObjectBuilder.start().add( "$where" , "this.a == 1" ).get() ) );
        assertNull( c.findOne( BasicDBObjectBuilder.start().add( "$where" , "this.a == 2" ).get() ) );
    }

    @Test
    public void testCodeWScope()
        throws MongoException {
        DBCollection c = _db.getCollection( "testCodeWScope" );
        c.drop();
        assertNull( c.findOne() );

        c.save( BasicDBObjectBuilder.start().add( "a" , 1 ).get() );
        assertNotNull( c.findOne() != null );

        assertNotNull( c.findOne( BasicDBObjectBuilder.start().add( "$where" , new CodeWScope( "this.a == x" , new BasicDBObject( "x" , 1 )  ) ).get() ) );
        assertNull( c.findOne( BasicDBObjectBuilder.start().add( "$where" , new CodeWScope( "this.a == x" , new BasicDBObject( "x" , 2 )  ) ).get() ) );


        c.drop();
        BasicDBObject in = new BasicDBObject();
        in.put( "_id" , 1 );
        in.put( "a" , new Code("x=5") );
        in.put( "b" , new CodeWScope( "x=5" , new BasicDBObject( "x" , 2 ) ) );
        c.insert( in );

        DBObject out = c.findOne();

        assertEquals( in , out );
    }


    @Test
    public void testCount()
        throws MongoException {
        DBCollection c = _db.getCollection("testCount");

        c.drop();
        assertNull(c.findOne());
        assertTrue(c.getCount() == 0);

        for (int i=0; i < 100; i++) {
            c.insert(new BasicDBObject("i", i));
        }

        assertEquals( 100 , c.getCount() );
        assertEquals( 100 , c.find().count() );
        assertEquals( 100 , c.find().limit(10).count() );
        assertEquals( 10 , c.find().limit(10).size() );
        assertEquals( 90 , c.find().skip(10).size() );
    }

    @Test
    public void testIndex()
        throws MongoException {
        DBCollection c = _db.getCollection("testIndex");

        c.drop();
        assertNull(c.findOne());

        for (int i=0; i < 100; i++) {
            c.insert(new BasicDBObject("i", i));
        }

        assertTrue(c.getCount() == 100);

        c.createIndex(new BasicDBObject("i", 1));

        List<DBObject> list = c.getIndexInfo();

        assertTrue(list.size() == 2);
        assertTrue(list.get(1).get("name").equals("i_1"));
    }

    @Test
    public void testBinary()
        throws MongoException {
        DBCollection c = _db.getCollection( "testBinary" );
        c.drop();
        c.save( BasicDBObjectBuilder.start().add( "a" , "eliot".getBytes() ).get() );

        DBObject out = c.findOne();
        byte[] b = (byte[])(out.get( "a" ) );
        assertEquals( "eliot" , new String( b ) );

        {
            byte[] raw = new byte[9];
            ByteBuffer bb = ByteBuffer.wrap( raw );
            bb.order( Bytes.ORDER );
            bb.putInt( 5 );
            bb.put( "eliot".getBytes() );
            out.put( "a" , "eliot".getBytes() );
            c.save( out );

            out = c.findOne();
            b = (byte[])(out.get( "a" ) );
            assertEquals( "eliot" , new String( b ) );

            out.put( "a" , new Binary( (byte)111 , raw ) );
            c.save( out );
            Binary blah = (Binary)c.findOne().get( "a" );
            assertEquals( 111 , blah.getType() );
            assertEquals( Util.toHex( raw ) , Util.toHex( blah.getData() ) );
        }

    }

    @Test
    public void testMinMaxKey()
        throws MongoException {
        DBCollection c = _db.getCollection( "testMinMaxKey" );
        c.drop();
        c.save( BasicDBObjectBuilder.start().add( "min" , new MinKey() ).add( "max" , new MaxKey() ).get() );

        DBObject out = c.findOne();
        MinKey min = (MinKey)(out.get( "min" ) );
        MaxKey max = (MaxKey)(out.get( "max" ) );
        assertTrue( JSON.serialize(min).contains("$minKey") );
        assertTrue( JSON.serialize(max).contains("$maxKey") );
    }

    @Test
    public void testBinaryOld()
        throws MongoException {
        DBCollection c = _db.getCollection( "testBinary" );
        c.drop();
        c.save( BasicDBObjectBuilder.start().add( "a" , "eliot".getBytes() ).get() );

        DBObject out = c.findOne();
        byte[] b = (byte[])(out.get( "a" ) );
        assertEquals( "eliot" , new String( b ) );

        {
            byte[] raw = new byte[9];
            ByteBuffer bb = ByteBuffer.wrap( raw );
            bb.order( Bytes.ORDER );
            bb.putInt( 5 );
            bb.put( "eliot".getBytes() );
            out.put( "a" , new Binary( BSON.B_BINARY , "eliot".getBytes() ) );
            c.save( out );

            // objects of subtype B_BINARY or B_GENERAL should becomes byte[]
            out = c.findOne();
//            Binary blah = (Binary)(out.get( "a" ) );
            byte[] bytes = (byte[]) out.get("a");
            assertEquals( "eliot" , new String( bytes ) );

            out.put( "a" , new Binary( (byte)111 , raw ) );
            c.save( out );
            Binary blah = (Binary)c.findOne().get( "a" );
            assertEquals( 111 , blah.getType() );
            assertEquals( Util.toHex( raw ) , Util.toHex( blah.getData() ) );
        }

    }
    @Test
    public void testUUID()
        throws MongoException {
        DBCollection c = _db.getCollection( "testUUID" );
        c.drop();
        c.save( BasicDBObjectBuilder.start().add( "a" , new UUID(1,2)).add("x",5).get() );

        DBObject out = c.findOne();
        UUID b = (UUID)(out.get( "a" ) );
        assertEquals( new UUID(1,2), b);
        assertEquals( 5 , out.get("x" ) );
    }

    @Test
    public void testEval()
        throws MongoException {
        assertEquals( 17 , ((Number)(_db.eval( "return 17" ))).intValue() );
        assertEquals( 18 , ((Number)(_db.eval( "function(x){ return 17 + x; }" , 1 ))).intValue() );
    }

    @Test
    public void testPartial1()
        throws MongoException {
        DBCollection c = _db.getCollection( "partial1" );
        c.drop();

        c.save( BasicDBObjectBuilder.start().add( "a" , 1 ).add( "b" , 2 ).get() );

        DBObject out = c.find().next();
        assertEquals( 1 , out.get( "a" ) );
        assertEquals( 2 , out.get( "b" ) );

        out = c.find( new BasicDBObject() , BasicDBObjectBuilder.start().add( "a" , 1 ).get() ).next();
        assertEquals( 1 , out.get( "a" ) );
        assertNull( out.get( "b" ) );

        out = c.find( null , BasicDBObjectBuilder.start().add( "a" , 1 ).get() ).next();
        assertEquals( 1 , out.get( "a" ) );
        assertNull( out.get( "b" ) );

        // make sure can't insert back partial
        try {
            c.update(out, out);
            assertTrue(false);
        } catch (IllegalArgumentException ex) {
        }

        out = c.findOne( null , BasicDBObjectBuilder.start().add( "b" , 1 ).get() );
        assertEquals( 2 , out.get( "b" ) );
        assertNull( out.get( "a" ) );

        // make sure can't insert back partial
        try {
            c.update(out, out);
            assertTrue(false);
        } catch (IllegalArgumentException ex) {
        }

    }

    @Test
    public void testGroup()
        throws MongoException {

        DBCollection c = _db.getCollection( "group1" );
        c.drop();
        c.save( BasicDBObjectBuilder.start().add( "x" , "a" ).get() );
        c.save( BasicDBObjectBuilder.start().add( "x" , "a" ).get() );
        c.save( BasicDBObjectBuilder.start().add( "x" , "a" ).get() );
        c.save( BasicDBObjectBuilder.start().add( "x" , "b" ).get() );

        DBObject g = c.group( new BasicDBObject( "x" , 1 ) , null , new BasicDBObject( "count" , 0 ) ,
                              "function( o , p ){ p.count++; }" );

        List l = (List)g;
        assertEquals( 2 , l.size() );
    }

    @Test
    public void testSet()
        throws MongoException {

        DBCollection c = _db.getCollection( "group1" );
        c.drop();
        c.save( BasicDBObjectBuilder.start().add( "id" , 1 ).add( "x" , true ).get() );
        assertEquals( Boolean.class , c.findOne().get( "x" ).getClass() );

        c.update( new BasicDBObject( "id" , 1 ) ,
                  new BasicDBObject( "$set" ,
                                     new BasicDBObject( "x" , 5.5 ) ) );
        assertEquals( Double.class , c.findOne().get( "x" ).getClass() );

    }

    @Test
    public void testKeys1()
        throws MongoException {

        DBCollection c = _db.getCollection( "keys1" );
        c.drop();
        c.save( BasicDBObjectBuilder.start().push( "a" ).add( "x" , 1 ).get() );

        assertEquals( 1, ((DBObject)c.findOne().get("a")).get("x" ) );
        c.update( new BasicDBObject() , BasicDBObjectBuilder.start().push( "$set" ).add( "a.x" , 2 ).get() );
        assertEquals( 1 , c.find().count() );
        assertEquals( 2, ((DBObject)c.findOne().get("a")).get("x" ) );

    }

    @Test
    public void testTimestamp()
        throws MongoException {

        DBCollection c = _db.getCollection( "ts1" );
        c.drop();
        c.save( BasicDBObjectBuilder.start().add( "y" , new BSONTimestamp() ).get() );

        BSONTimestamp t = (BSONTimestamp)c.findOne().get("y");
        assert( t.getTime() > 0 );
        assert( t.getInc() > 0 );
    }

    @Test
    public void testStrictWriteSetInCollection(){
        DBCollection c = _db.getCollection( "write1" );
        c.drop();
        c.setWriteConcern( WriteConcern.SAFE);
        c.insert( new BasicDBObject( "_id" , 1 ) );
        boolean gotError = false;
        try {
            c.insert( new BasicDBObject( "_id" , 1 ) );
        }
        catch ( MongoException.DuplicateKey e ){
            gotError = true;
        }
        assertEquals( true , gotError );

        assertEquals( 1 , c.find().count() );
    }

    @Test
    public void testStrictWriteSetInMethod(){
        DBCollection c = _db.getCollection( "write1" );
        c.drop();
        c.insert( new BasicDBObject( "_id" , 1 ));
        boolean gotError = false;
        try {
            c.insert( new BasicDBObject( "_id" , 1 ) , WriteConcern.SAFE);
        }
        catch ( MongoException.DuplicateKey e ){
            gotError = true;
        }
        assertEquals( true , gotError );

        assertEquals( 1 , c.find().count() );
    }

    @Test
    public void testPattern(){
        DBCollection c = _db.getCollection( "jp1" );
        c.drop();

        c.insert( new BasicDBObject( "x" , "a" ) );
        c.insert( new BasicDBObject( "x" , "A" ) );

        assertEquals( 1 , c.find( new BasicDBObject( "x" , Pattern.compile( "a" ) ) ).itcount() );
        assertEquals( 1 , c.find( new BasicDBObject( "x" , Pattern.compile( "A" ) ) ).itcount() );
        assertEquals( 2 , c.find( new BasicDBObject( "x" , Pattern.compile( "a" , Pattern.CASE_INSENSITIVE ) ) ).itcount() );
        assertEquals( 2 , c.find( new BasicDBObject( "x" , Pattern.compile( "A" , Pattern.CASE_INSENSITIVE ) ) ).itcount() );
    }


    @Test
    public void testDates(){
        DBCollection c = _db.getCollection( "dates1" );
        c.drop();

        DBObject in = new BasicDBObject( "x" , new java.util.Date() );
        c.insert( in );
        DBObject out = c.findOne();
        assertEquals( java.util.Date.class , in.get("x").getClass() );
        assertEquals( in.get( "x" ).getClass() , out.get( "x" ).getClass() );
    }

    @Test
    public void testMapReduce(){
        DBCollection c = _db.getCollection( "jmr1" );
        c.drop();

        c.save( new BasicDBObject( "x" , new String[]{ "a" , "b" } ) );
        c.save( new BasicDBObject( "x" , new String[]{ "b" , "c" } ) );
        c.save( new BasicDBObject( "x" , new String[]{ "c" , "d" } ) );

        MapReduceOutput out =
            c.mapReduce( "function(){ for ( var i=0; i<this.x.length; i++ ){ emit( this.x[i] , 1 ); } }" ,
                         "function(key,values){ var sum=0; for( var i=0; i<values.length; i++ ) sum += values[i]; return sum;}" ,
                         "jmr1_out" , null );

        Map<String,Integer> m = new HashMap<String,Integer>();
        for ( DBObject r : out.results() ){
            m.put( r.get( "_id" ).toString() , ((Number)(r.get( "value" ))).intValue() );
        }

        assertEquals( 4 , m.size() );
        assertEquals( 1 , m.get( "a" ).intValue() );
        assertEquals( 2 , m.get( "b" ).intValue() );
        assertEquals( 2 , m.get( "c" ).intValue() );
        assertEquals( 1 , m.get( "d" ).intValue() );

    }

    @Test
    public void testMapReduceInline(){
        DBCollection c = _db.getCollection( "imr1" );
        c.drop();

        c.save( new BasicDBObject( "x" , new String[]{ "a" , "b" } ) );
        c.save( new BasicDBObject( "x" , new String[]{ "b" , "c" } ) );
        c.save( new BasicDBObject( "x" , new String[]{ "c" , "d" } ) );

        MapReduceOutput out =
            c.mapReduce( "function(){ for ( var i=0; i<this.x.length; i++ ){ emit( this.x[i] , 1 ); } }" ,
                         "function(key,values){ var sum=0; for( var i=0; i<values.length; i++ ) sum += values[i]; return sum;}" , 
                         null, MapReduceCommand.OutputType.INLINE, null, ReadPreference.primaryPreferred());

        Map<String,Integer> m = new HashMap<String,Integer>();
        for ( DBObject r : out.results() ){
            m.put( r.get( "_id" ).toString() , ((Number)(r.get( "value" ))).intValue() );
        }

        assertEquals( 4 , m.size() );
        assertEquals( 1 , m.get( "a" ).intValue() );
        assertEquals( 2 , m.get( "b" ).intValue() );
        assertEquals( 2 , m.get( "c" ).intValue() );
        assertEquals( 1 , m.get( "d" ).intValue() );

    }


    //If run against a replicaset this will verify that the inline map/reduce hits the secondary.
    @Test
    @SuppressWarnings("deprecation")
    public void testMapReduceInlineSecondary() throws Exception {
        Mongo mongo = new MongoClient(Arrays.asList(new ServerAddress("127.0.0.1", 27017), new ServerAddress("127.0.0.1", 27018)),
                MongoClientOptions.builder().writeConcern(WriteConcern.UNACKNOWLEDGED).build());

        if (isStandalone(mongo)) {
            return;
        }

        int size = getReplicaSetSize(mongo);
        DBCollection c = mongo.getDB(_db.getName()).getCollection( "imr2" );
        //c.setReadPreference(ReadPreference.SECONDARY);
        c.slaveOk();
        c.drop();

        c.save( new BasicDBObject( "x" , new String[]{ "a" , "b" } ) );
        c.save( new BasicDBObject( "x" , new String[]{ "b" , "c" } ) );
        WriteResult wr = c.save( new BasicDBObject( "x" , new String[]{ "c" , "d" } ) );
        if (mongo.getReplicaSetStatus() != null  && mongo.getReplicaSetStatus().getName() != null) {
            wr.getLastError(new WriteConcern(size));
        }

        MapReduceOutput out =
            c.mapReduce( "function(){ for ( var i=0; i<this.x.length; i++ ){ emit( this.x[i] , 1 ); } }" ,
                         "function(key,values){ var sum=0; for( var i=0; i<values.length; i++ ) sum += values[i]; return sum;}" , null, MapReduceCommand.OutputType.INLINE, null);

        Map<String,Integer> m = new HashMap<String,Integer>();
        for ( DBObject r : out.results() ){
            m.put( r.get( "_id" ).toString() , ((Number)(r.get( "value" ))).intValue() );
        }

        assertEquals( 4 , m.size() );
        assertEquals( 1 , m.get( "a" ).intValue() );
        assertEquals( 2 , m.get( "b" ).intValue() );
        assertEquals( 2 , m.get( "c" ).intValue() );
        assertEquals( 1 , m.get( "d" ).intValue() );
        ReplicaSetStatus replStatus = mongo.getReplicaSetStatus();
        //if it is a replicaset, and there is no master, or master is not the secondary
        if( replStatus!= null && replStatus.getName() != null && ((replStatus.getMaster() == null) || (replStatus.getMaster() != null && !replStatus.getMaster().equals(replStatus.getASecondary()))))
            assertTrue( !mongo.getReplicaSetStatus().isMaster( out.getCommandResult().getServerUsed() ),
            		"Had a replicaset but didn't use secondary! replSetStatus : " + mongo.getReplicaSetStatus() + " \n Used: " + out.getCommandResult().getServerUsed() + " \n ");
    }

    @Test
    public void testMapReduceInlineWScope(){
        DBCollection c = _db.getCollection( "jmr2" );
        c.drop();

        c.save( new BasicDBObject( "x" , new String[]{ "a" , "b" } ) );
        c.save( new BasicDBObject( "x" , new String[]{ "b" , "c" } ) );
        c.save( new BasicDBObject( "x" , new String[]{ "c" , "d" } ) );

        Map<String, Object> scope = new HashMap<String, Object>();
        scope.put("exclude", "a");

        MapReduceCommand mrc = new MapReduceCommand( c, "function(){ for ( var i=0; i<this.x.length; i++ ){ if(this.x[i] != exclude) emit( this.x[i] , 1 ); } }" ,
                         "function(key,values){ var sum=0; for( var i=0; i<values.length; i++ ) sum += values[i]; return sum;}" , null, MapReduceCommand.OutputType.INLINE, null);
        mrc.setScope( scope );

        MapReduceOutput out = c.mapReduce( mrc );
        Map<String,Integer> m = new HashMap<String,Integer>();
        for ( DBObject r : out.results() ){
            m.put( r.get( "_id" ).toString() , ((Number)(r.get( "value" ))).intValue() );
        }

        assertEquals( 3 , m.size() );
        assertEquals( 2 , m.get( "b" ).intValue() );
        assertEquals( 2 , m.get( "c" ).intValue() );
        assertEquals( 1 , m.get( "d" ).intValue() );

    }
    
    @Test
    public void testAggregation(){
        if (!serverIsAtLeastVersion(2.1)) {
            return;
        }

        DBCollection c = _db.getCollection( "aggregationTest" );
        c.drop();
        
        DBObject foo = new BasicDBObject( "name" , "foo" ) ;
        DBObject bar = new BasicDBObject( "name" , "bar" ) ;
        DBObject baz = new BasicDBObject( "name" , "foo" ) ;
        foo.put( "count", 5 );
        bar.put( "count", 2 );
        baz.put( "count", 7 );
        c.insert( foo );
        c.insert( bar );
        c.insert( baz );
        
        DBObject projFields = new BasicDBObject( "name", 1 );
        projFields.put("count", 1);
        
        DBObject group = new BasicDBObject( );
        group.put("_id", "$name" );
        group.put( "docsPerName", new BasicDBObject( "$sum", 1 ));
        group.put( "countPerName", new BasicDBObject( "$sum", "$count" ));
        
        AggregationOutput out = c.aggregate( new BasicDBObject( "$project", projFields ), new BasicDBObject( "$group", group) );
        
        Map<String, DBObject> results = new HashMap<String, DBObject>();
        for(DBObject result : out.results())
            results.put((String)result.get("_id"), result);
        
        DBObject fooResult = results.get("foo");
        assertNotNull(fooResult);
        assertEquals(2, fooResult.get("docsPerName"));
        assertEquals(12, fooResult.get("countPerName"));
        
        DBObject barResult = results.get("bar");
        assertNotNull(barResult);
        assertEquals(1, barResult.get("docsPerName"));
        assertEquals(2, barResult.get("countPerName"));
        
       DBObject aggregationCommand = out.getCommand();
       assertNotNull(aggregationCommand);
       assertEquals(c.getName(), aggregationCommand.get("aggregate"));
       assertNotNull(aggregationCommand.get("pipeline"));
    }

    String _testMulti( DBCollection c ){
        String s = "";
        for ( DBObject z : c.find().sort( new BasicDBObject( "_id" , 1 ) ) ){
            if ( s.length() > 0 )
                s += ",";
            s += z.get( "x" );
        }
        return s;
    }

    @Test
    public void testMulti(){
        DBCollection c = _db.getCollection( "multi1" );
        c.drop();

        c.insert( BasicDBObjectBuilder.start( "_id" , 1 ).add( "x" , 1 ).get() );
        c.insert( BasicDBObjectBuilder.start( "_id" , 2 ).add( "x" , 5 ).get() );

        assertEquals( "1,5" , _testMulti( c ) );

        c.update( new BasicDBObject() , BasicDBObjectBuilder.start().push( "$inc" ).add( "x" , 1 ).get() );
        assertEquals( "2,5" , _testMulti( c ) );

        c.update( new BasicDBObject( "_id" , 2 ) , BasicDBObjectBuilder.start().push( "$inc" ).add( "x" , 1 ).get() );
        assertEquals( "2,6" , _testMulti( c ) );

        c.updateMulti( new BasicDBObject() , BasicDBObjectBuilder.start().push( "$inc" ).add( "x" , 1 ).get() );
        assertEquals( "3,7" , _testMulti( c ) );

    }

    @Test
    public void testAuthenticate() throws UnknownHostException {
        assertEquals( "26e3d12bd197368526409177b3e8aab6" , _db._hash( "e" , "j".toCharArray() ) );

        Mongo m = new MongoClient();
        DB db = m.getDB(cleanupDB);
        DBCollection usersCollection = db.getCollection( "system.users" );

        try {
            usersCollection.remove(new BasicDBObject());
            assertEquals(0, usersCollection.find().count());

            db.addUser("xx" , "e".toCharArray() );
            assertEquals(1, usersCollection.find().count());

            assertEquals(false, db.authenticate( "xx" , "f".toCharArray() ) );
            assertNull(db.getAuthenticationCredentials());
            assertNull(_mongo.getAuthority().getCredentialsStore().get(db.getName()));
            assertEquals(true, db.authenticate("xx", "e".toCharArray()));
            assertEquals(MongoCredential.createMongoCRCredential("xx", db.getName(), "e".toCharArray()), db.getAuthenticationCredentials());
            assertEquals(db.getAuthenticationCredentials(), m.getAuthority().getCredentialsStore().get(db.getName()));

            assertEquals(true, db.authenticate( "xx" , "e".toCharArray() ) );
            try {
                db.authenticateCommand("xx", "f".toCharArray());
                fail("can't auth with different credentials");
            } catch (IllegalStateException e) {
                // all good;
            }
        }
        finally {
            usersCollection.remove( new BasicDBObject() );
            m.close();
        }
    }

    @Test
    public void testAuthenticateCommand() throws UnknownHostException {
        Mongo m = new MongoClient();
        DB db = m.getDB(cleanupDB);
        DBCollection usersCollections = db.getCollection( "system.users" );

        try {
            usersCollections.remove(new BasicDBObject());
            assertEquals(0, usersCollections.find().count());

            db.addUser("xx", "e".toCharArray());
            assertEquals(1, usersCollections.find().count());

            try {
                db.authenticateCommand( "xx" , "f".toCharArray());
                fail("Auth should have failed");
            } catch (CommandFailureException e) {
                // all good
            }
            assertTrue(db.authenticateCommand("xx", "e".toCharArray()).ok());
            assertTrue(db.authenticateCommand("xx", "e".toCharArray()).ok());
            try {
                db.authenticateCommand("xx", "f".toCharArray());
                fail("can't auth with different credentials");
            } catch (IllegalStateException e) {
                // all good;
            }
        }
        finally {
            usersCollections.remove(new BasicDBObject());
            m.close();
        }
    }

    @Test
    public void testAuthenticateWithCredentialsInURIAndNoDatabase() throws UnknownHostException {
        // First add the user
        Mongo m = new MongoClient(new MongoClientURI("mongodb://localhost"));
        DB db = m.getDB("admin");
        DBCollection usersCollection = db.getCollection( "system.users" );
        try {
            usersCollection.remove(new BasicDBObject());
            assertEquals(0, usersCollection.find().count());

            db.addUser("xx", "e".toCharArray());
        }
        finally {
            m.close();
        }

        m = new MongoClient(new MongoClientURI("mongodb://xx:e@localhost"));
        db = m.getDB("admin");

        try {
            assertEquals(1, m.getDB("admin").getCollection("system.users").find().count());
            assertNotNull(db.getAuthenticationCredentials());
            assertEquals(true, db.authenticate("xx", "e".toCharArray()) );
        }
        finally {
            db.getCollection( "system.users" ).remove(new BasicDBObject());
            m.close();
        }
    }

    @Test
    public void testAuthenticateWithCredentialsInURI() throws UnknownHostException {
        // First add the user
        Mongo m = new MongoClient(new MongoClientURI("mongodb://localhost"));
        DB db = m.getDB(cleanupDB);
        DBCollection usersCollection = db.getCollection( "system.users" );
        try {
            usersCollection.remove(new BasicDBObject());
            assertEquals(0, usersCollection.find().count());

            db.addUser("xx", "e".toCharArray());
            assertEquals(1, usersCollection.find().count());
        }
        finally {
            m.close();
        }

        m = new MongoClient(new MongoClientURI("mongodb://xx:e@localhost/" + cleanupDB));
        db = m.getDB(cleanupDB);

        try {
            assertNotNull(db.getAuthenticationCredentials());
            assertEquals(true, db.authenticate("xx", "e".toCharArray()) );
        }
        finally {
            db.getCollection( "system.users" ).remove(new BasicDBObject());
            m.close();
        }
    }

    @Test
    public void testAuthenticateCommandWithCredentialsInURI() throws UnknownHostException {
        // First add the user
        Mongo m = new MongoClient(new MongoClientURI("mongodb://localhost"));
        DB db = m.getDB(cleanupDB);
        DBCollection usersCollection = db.getCollection( "system.users" );
        try {
            usersCollection.remove(new BasicDBObject());
            assertEquals(0, usersCollection.find().count());

            db.addUser("xx", "e".toCharArray());
            assertEquals(1, usersCollection.find().count());
        }
        finally {
            m.close();
        }

        m = new MongoClient(new MongoClientURI("mongodb://xx:e@localhost/" + cleanupDB));
        db = m.getDB(cleanupDB);

        try {
            assertNotNull(db.getAuthenticationCredentials());
            assertTrue(db.authenticateCommand("xx", "e".toCharArray()).ok());
        }
        finally {
            db.getCollection( "system.users" ).remove(new BasicDBObject());
            m.close();
        }
    }

    @Test
    public void testTransformers(){
        DBCollection c = _db.getCollection( "tt" );
        c.drop();

        c.save( BasicDBObjectBuilder.start( "_id" , 1 ).add( "x" , 1.1 ).get() );
        assertEquals( Double.class , c.findOne().get( "x" ).getClass() );

        Bytes.addEncodingHook( Double.class , new Transformer(){
            public Object transform( Object o ){
                return o.toString();
            }
        } );

        c.save( BasicDBObjectBuilder.start( "_id" , 1 ).add( "x" , 1.1 ).get() );
        assertEquals( String.class , c.findOne().get( "x" ).getClass() );

        Bytes.clearAllHooks();
        c.save( BasicDBObjectBuilder.start( "_id" , 1 ).add( "x" , 1.1 ).get() );
        assertEquals( Double.class , c.findOne().get( "x" ).getClass() );

        Bytes.addDecodingHook( Double.class , new Transformer(){
            public Object transform( Object o ){
                return o.toString();
            }
        } );
        assertEquals( String.class , c.findOne().get( "x" ).getClass() );
        Bytes.clearAllHooks();
        assertEquals( Double.class , c.findOne().get( "x" ).getClass() );
    }


    @Test
    public void testObjectIdCompat(){
        DBCollection c = _db.getCollection( "oidc" );
        c.drop();

        c.save( new BasicDBObject( "x" , 1 ) );
        _db.eval( "db.oidc.insert( { x : 2 } );" );

        List<DBObject> l = c.find().toArray();
        assertEquals( 2 , l.size() );

        ObjectId a = (ObjectId)(l.get(0).get("_id"));
        ObjectId b = (ObjectId)(l.get(1).get("_id"));

        assertLess( Math.abs( a.getTime() - b.getTime() ) , 10000 );
    }

    @Test
    public void testObjectIdCompat2(){
        DBCollection c = _db.getCollection( "oidc" );
        c.drop();

        c.save( new BasicDBObject( "x" , 1 ) );

        String o = (String) _db.eval( "return db.oidc.findOne()._id.toString()" );
        // printing on servers has changed in 2.1
        if (o.startsWith("ObjectId"))
            o = (String) _db.eval( "return db.oidc.findOne()._id.valueOf()" );
        String x = c.findOne().get( "_id" ).toString();
        assertEquals( x , o );
    }


    @Test
    public void testLargeBulkInsert(){
        // max size should be obtained from server
        int maxObjSize = _mongo.getMaxBsonObjectSize();
        DBCollection c = _db.getCollection( "largebulk" );
        c.drop();
        String s = "asdasdasd";
        while ( s.length() < 10000 )
            s += s;
        List<DBObject> l = new ArrayList<DBObject>();
        final int num = 3 * ( maxObjSize / s.length() );

        for ( int i=0; i<num; i++ ){
            l.add( BasicDBObjectBuilder.start()
                   .add( "_id" , i )
                   .add( "x" , s )
                   .get() );
        }
        assertEquals( 0 , c.find().count() );
        c.insert( l );
        assertEquals(num, c.find().count());

        try {
            c.save( new BasicDBObject( "foo" , new Binary(new byte[maxObjSize]) ) );
            fail("Should not be able to save an object larger than maximum bson object size of " + maxObjSize);
        }
        catch ( MongoInternalException ie ) {
            assertEquals(-3, ie.getCode());
        }
        assertEquals( num , c.find().count() );
    }

    @Test
    public void testUpdate5(){
        DBCollection c = _db.getCollection( "udpate5" );
        c.drop();

        c.insert( new BasicDBObject( "x" , 5) );
        assertEquals(Integer.class, c.findOne().get("x").getClass());
        assertEquals(5, c.findOne().get("x"));

        c.update(new BasicDBObject(), new BasicDBObject("$set", new BasicDBObject("x", 5.6D)));
        assertEquals( Double.class , c.findOne().get("x").getClass() );
        assertEquals( 5.6 , c.findOne().get("x") );


    }

    @Test
    public void testIn(){
        DBCollection c = _db.getCollection( "in1" );
        c.drop();

        c.insert( new BasicDBObject( "x" , 1 ) );
        c.insert( new BasicDBObject( "x" , 2 ) );
        c.insert(new BasicDBObject("x", 3));
        c.insert(new BasicDBObject("x", 4));

        List<DBObject> a = c.find( new BasicDBObject( "x" , new BasicDBObject( "$in" , new Integer[]{ 2 , 3 } ) ) ).toArray();
        assertEquals( 2 , a.size() );
    }

    @Test
    public void testWriteResultWithGetLastErrorWithDifferentConcerns(){
        DBCollection c = _db.getCollection( "writeresultwfle1" );
        c.drop();

        c.insert( new BasicDBObject( "_id" , 1 ) );
        WriteResult res = c.update( new BasicDBObject( "_id" , 1 ) , new BasicDBObject( "$inc" , new BasicDBObject( "x" , 1 ) ) );
        assertEquals( 1 , res.getN() );
        assertTrue( res.isLazy() );

        CommandResult cr = res.getLastError( WriteConcern.FSYNC_SAFE );
        assertEquals( 1 , cr.getInt( "n" ) );
        assertTrue(cr.containsField("fsyncFiles") || cr.containsField("waited"));

        CommandResult cr2 = res.getLastError( WriteConcern.FSYNC_SAFE );
        assertTrue( cr == cr2 );

        CommandResult cr3 = res.getLastError( WriteConcern.NONE );
        assertTrue( cr3 == cr );

    }

    @Test
    public void testWriteResult(){
        DBCollection c = _db.getCollection( "writeresult1" );
        c.drop();

        c.insert( new BasicDBObject( "_id" , 1 ) );
        WriteResult res = c.update( new BasicDBObject( "_id" , 1 ) , new BasicDBObject( "$inc" , new BasicDBObject( "x" , 1 ) ) );
        assertEquals( 1 , res.getN() );
        assertTrue( res.isLazy() );

        c.setWriteConcern( WriteConcern.SAFE);
        res = c.update( new BasicDBObject( "_id" , 1 ) , new BasicDBObject( "$inc" , new BasicDBObject( "x" , 1 ) ) );
        assertEquals( 1 , res.getN() );
        assertFalse( res.isLazy() );
    }

    @Test
    public void testWriteResultMethodLevelWriteConcern(){
        DBCollection c = _db.getCollection( "writeresult2" );
        c.drop();

        c.insert( new BasicDBObject( "_id" , 1 ) );
        WriteResult res = c.update( new BasicDBObject( "_id" , 1 ) , new BasicDBObject( "$inc" , new BasicDBObject( "x" , 1 ) ) );
        assertEquals( 1 , res.getN() );
        assertTrue(res.isLazy());

        res = c.update( new BasicDBObject( "_id" , 1 ) , new BasicDBObject( "$inc" , new BasicDBObject( "x" , 1 ) ) , false , false , WriteConcern.SAFE);
        assertEquals( 1 , res.getN() );
        assertFalse(res.isLazy());
    }

    @Test
    public void testWriteConcernValueOf() {
        WriteConcern wc1 = WriteConcern.NORMAL;
        WriteConcern wc2 = WriteConcern.valueOf( "normal" );
        WriteConcern wc3 = WriteConcern.valueOf( "NORMAL" );

        assertEquals( wc1, wc2 );
        assertEquals( wc1, wc3 );
        assertEquals( wc1.getW(), wc2.getW() );
        assertEquals( wc1.getWObject(), wc2.getWObject() );
        assertEquals( wc1.getW(), wc3.getW() );
        assertEquals( wc1.getWObject(), wc3.getWObject() );
    }

    @Test
    public void testWriteConcernMajority() {
        WriteConcern wc1 = WriteConcern.MAJORITY;
        WriteConcern wc2 = WriteConcern.valueOf( "majority" );
        WriteConcern wc3 = WriteConcern.valueOf( "MAJORITY" );

        assertEquals( wc1, wc2 );
        assertEquals( wc1, wc3 );
        assertEquals( wc1.getWString(), wc2.getWString() );
        assertEquals( wc1.getWObject(), wc2.getWObject() );
        assertEquals( wc1.getWString(), wc3.getWString() );
        assertEquals( wc1.getWObject(), wc3.getWObject() );
    }

    @Test
    public void testFindAndModify(){
        DBCollection c = _db.getCollection( "findandmodify" );
        c.drop();

        c.insert( new BasicDBObject( "_id" , 1 ) );
        //return old one
        DBObject dbObj = c.findAndModify( new BasicDBObject( "_id" , 1 ) , null, new BasicDBObject( "x", 1));
        assertEquals( 1 , dbObj.keySet().size());
        assertEquals( 1 , c.findOne(new BasicDBObject( "_id" , 1 ) ).get( "x" ));

        //return new one
        dbObj = c.findAndModify( new BasicDBObject( "_id" , 1 ) , null, null, false, new BasicDBObject( "x", 5), true, false);
        assertEquals( 2 , dbObj.keySet().size());
        assertEquals( 5 , dbObj.get( "x" ));
        assertEquals( 5 , c.findOne(new BasicDBObject( "_id" , 1 ) ).get( "x" ));

        //remove it, and return old one
        dbObj = c.findAndRemove( new BasicDBObject( "_id" , 1 ) );
        assertEquals( 2 , dbObj.keySet().size());
        assertEquals( 5 , dbObj.get( "x" ));
        assertNull( c.findOne(new BasicDBObject( "_id" , 1 ) ));

        // create new one with upsert and return it
        dbObj = c.findAndModify( new BasicDBObject( "_id" , 2 ) , null, null, false, new BasicDBObject("$set", new BasicDBObject("a", 6)), true, true);
        assertEquals( 2 , dbObj.keySet().size());
        assertEquals( 6 , dbObj.get( "a" ));
        assertEquals( 6 , c.findOne(new BasicDBObject( "_id" , 2 ) ).get( "a" ));

        // create new one with upsert and don't return it
        dbObj = c.findAndModify( new BasicDBObject( "_id" , 3 ) , null, null, false, new BasicDBObject("$set", new BasicDBObject("b", 7)), false, true);

        assertEquals( 7 , c.findOne(new BasicDBObject( "_id" , 3 ) ).get( "b" ));
        if (serverIsAtLeastVersion(2.1)) {
            assertNull(dbObj);
        } else {
            assertEquals(0, dbObj.keySet().size());
        }

            // test exception throwing
        c.insert( new BasicDBObject( "a" , 1 ) );
        try {
            c.findAndModify( null, null );
            fail("Exception not thrown when no update nor remove");
        } catch (MongoException e) {
        }

        try {
            dbObj = c.findAndModify( new BasicDBObject("a", "noexist"), null );
            if (!serverIsAtLeastVersion(2.1)) {
               assertNull(dbObj);
            }
        } catch (MongoException e) {
            if (!serverIsAtLeastVersion(2.1)) {
                fail("Exception thrown when matching record");
            }
        }
    }

    @Test
    public void testGetCollectionFromString(){
        DBCollection c = _db.getCollectionFromString( "foo" );
        assertEquals( "foo" , c.getName() );

        c = _db.getCollectionFromString( "foo.bar" );
        assertEquals( "foo.bar" , c.getName() );

        c = _db.getCollectionFromString( "foo.bar.zoo" );
        assertEquals( "foo.bar.zoo" , c.getName() );

        c = _db.getCollectionFromString( "foo.bar.zoo.dork" );
        assertEquals( "foo.bar.zoo.dork" , c.getName() );

    }

    @Test
    public void testBadKey(){
        DBCollection c = _db.getCollectionFromString( "foo" );
        assertEquals( "foo" , c.getName() );

        try {
            c.insert(new BasicDBObject("a.b", 1));
            fail("Bad key was accepted");
        } catch (IllegalArgumentException e) {}

        try {
            Map<String, Integer> data = new HashMap<String, Integer>();
            data.put("a.b", 1);
            c.insert(new BasicDBObject("data", data));
            fail("Bad key was accepted");
        } catch (IllegalArgumentException e) {}

        try {
            c.insert(new BasicDBObject("$a", 1));
            fail("Bad key was accepted");
        } catch (IllegalArgumentException e) {}

        try {
            c.save(new BasicDBObject("a.b", 1));
            fail("Bad key was accepted");
        } catch (IllegalArgumentException e) {}

        try {
            c.save(new BasicDBObject("$a", 1));
            fail("Bad key was accepted");
        } catch (IllegalArgumentException e) {}

        try {
            final BasicDBList list = new BasicDBList();
            list.add(new BasicDBObject("$a", 1));
            c.save(new BasicDBObject("a", list));
            fail("Bad key was accepted");
        } catch (IllegalArgumentException e) {}

        try {
            final List<BasicDBObject> list = Arrays.asList(new BasicDBObject("$a", 1));
            c.save(new BasicDBObject("a", list));
            fail("Bad key was accepted");
        } catch (IllegalArgumentException e) {}

//        try {
//            c.save(new BasicDBObject("a", Arrays.asList(new BasicDBObject("$a", 1))));
//            fail("Bad key was accepted");
//        } catch (IllegalArgumentException e) {}

        c.insert(new BasicDBObject("a", 1));
        try {
            c.update(new BasicDBObject("a", 1), new BasicDBObject("a.b", 1));
            fail("Bad key was accepted");
        } catch (IllegalArgumentException e) {}

        // this should work because it's a query
        c.update(new BasicDBObject("a", 1), new BasicDBObject("$set", new BasicDBObject("a.b", 1)));
    }

    @Test
    public void testAllTypes(){
        DBCollection c = _db.getCollectionFromString( "foo" );
        c.drop();
        String json = "{ 'str' : 'asdfasd' , 'long' : 5 , 'float' : 0.4 , 'bool' : false , 'date' : { '$date' : '2011-05-18T18:56:00Z'} , 'pat' : { '$regex' : '.*' , '$options' : ''} , 'oid' : { '$oid' : '4d83ab3ea39562db9c1ae2ae'} , 'ref' : { '$ref' : 'test.test' , '$id' : { '$oid' : '4d83ab59a39562db9c1ae2af'}} , 'code' : { '$code' : 'asdfdsa'} , 'codews' : { '$code' : 'ggggg' , '$scope' : { }} , 'ts' : { '$ts' : 1300474885 , '$inc' : 10} , 'null' :  null, 'uuid' : { '$uuid' : '60f65152-6d4a-4f11-9c9b-590b575da7b5' }}";
        BasicDBObject a = (BasicDBObject) JSON.parse(json);
        c.insert(a);
        DBObject b = c.findOne();
        assertTrue(a.equals(b));
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testMongoHolder() throws MongoException, UnknownHostException {
        Mongo m1 = Mongo.Holder.singleton().connect( new MongoURI( "mongodb://localhost" ) );
        Mongo m2 = Mongo.Holder.singleton().connect( new MongoURI( "mongodb://localhost" ) );

        assertEquals( m1, m2);
    }
    final Mongo _mongo;
    final DB _db;

    public static void main( String args[] )
        throws Exception {
        JavaClientTest ct = new JavaClientTest();
        ct.runConsole();
    }
}
