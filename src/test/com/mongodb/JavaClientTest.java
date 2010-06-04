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

import java.io.*;
import java.nio.*;
import java.util.*;
import java.util.regex.*;

import org.bson.*;
import org.bson.types.*;
import org.testng.annotations.*;

import com.mongodb.util.*;

public class JavaClientTest extends TestCase {
    
    public JavaClientTest()
        throws IOException , MongoException {
        _mongo = new Mongo( "127.0.0.1" );
	cleanupMongo = _mongo;
	cleanupDB = "com_mongodb_unittest_JavaClientTest";
	_db = cleanupMongo.getDB( cleanupDB );
    }

    @Test
    public void test1()
        throws MongoException {
        DBCollection c = _db.getCollection( "test1" );;
        c.drop();

        DBObject m = new BasicDBObject();
        m.put( "name" , "eliot" );
        m.put( "state" , "ny" );
        
        c.save( m );
        System.out.println( m.keySet() );
        assert( m.containsField( "_id" ) );

        Map out = (Map)(c.findOne( m.get( "_id" )));
        assertEquals( "eliot" , out.get( "name" ) );
        assertEquals( "ny" , out.get( "state" ) );
    }

    @Test
    public void test2()
        throws MongoException {
        DBCollection c = _db.getCollection( "test2" );;
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

        assertTrue(c.getCount() == 100);
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
            out.put( "a" , new Binary( (byte)2 , raw ) );
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
    public void testUUID()
        throws MongoException {
        DBCollection c = _db.getCollection( "testUUID" );
        c.drop();
        c.save( BasicDBObjectBuilder.start().add( "a" , new UUID(1,2)).get() );
        
        DBObject out = c.findOne();
        UUID b = (UUID)(out.get( "a" ) );
        assertEquals( new UUID(1,2), b);
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
    public void testStrictWrite(){
        DBCollection c = _db.getCollection( "write1" );
        c.drop();
        c.setWriteConcern( DB.WriteConcern.STRICT );
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
                         null , null );
        
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
    public void testAuth(){
        assertEquals( "26e3d12bd197368526409177b3e8aab6" , _db._hash( "e" , new char[]{ 'j' } ) );

        DBCollection u = _db.getCollection( "system.users" );

        try {
            assertEquals( 0 , u.find().count() );
            
            _db.addUser( "xx" , new char[]{ 'e' } );
            assertEquals( 1 , u.find().count() );
            
            assertEquals( false , _db.authenticate( "xx" , new char[]{ 'f' } ) );
            assertEquals( true , _db.authenticate( "xx" , new char[]{ 'e' } ) );
        }
        finally {
            u.remove( new BasicDBObject() );
            assertEquals( 0 , u.find().count() );        
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
        
        Object o = _db.eval( "return db.oidc.findOne()._id.toString()" );
        String x = c.findOne().get( "_id" ).toString();
        assertEquals( x , o );
    }
       
 
    @Test
    public void testLargeBulkInsert(){
        DBCollection c = _db.getCollection( "largebulk" );
        c.drop();
        String s = "asdasdasd";
        while ( s.length() < 10000 )
            s += s;
        List<DBObject> l = new ArrayList<DBObject>();
        final int num = 3 * ( Bytes.MAX_OBJECT_SIZE / s.length() );

        for ( int i=0; i<num; i++ ){
            l.add( BasicDBObjectBuilder.start()
                   .add( "_id" , i )
                   .add( "x" , s )
                   .get() );
        }
        assertEquals( 0 , c.find().count() );
        c.insert( l );
        assertEquals( num , c.find().count() );

        s = l.toString();
        assertTrue( s.length() > Bytes.MAX_OBJECT_SIZE );
        
        boolean worked = false;
        try {
            c.save( new BasicDBObject( "foo" , s ) );
            worked = true;
        }
        catch ( IllegalArgumentException ie ){}
        assertFalse( worked );

        assertEquals( num , c.find().count() );
    }

    @Test
    public void testUpdate5(){
        DBCollection c = _db.getCollection( "udpate5" );
        c.drop();
        
        c.insert( new BasicDBObject( "x" , new Integer( 5 ) ) );
        assertEquals( Integer.class , c.findOne().get("x").getClass() );
        assertEquals( new Integer(5) , c.findOne().get("x") );

        c.update( new BasicDBObject() , new BasicDBObject( "$set" , new BasicDBObject( "x" , 5.6D ) ) );
        assertEquals( Double.class , c.findOne().get("x").getClass() );
        assertEquals( 5.6 , c.findOne().get("x") );
        
        
    }

    final Mongo _mongo;
    final DB _db;

    public static void main( String args[] )
        throws Exception {
        JavaClientTest ct = new JavaClientTest();
        ct.runConsole();
    }
}
