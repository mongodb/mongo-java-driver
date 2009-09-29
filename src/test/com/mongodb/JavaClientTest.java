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
import java.util.*;

import org.testng.annotations.Test;

import com.mongodb.util.*;

public class JavaClientTest extends TestCase {
    
    public JavaClientTest()
        throws IOException , MongoException {
        _db = new Mongo( "127.0.0.1" , "jtest" );        
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
    public void testCount()
        throws MongoException {
        DBCollection c = _db.getCollection("testCount");

        c.drop();
        assertNull(c.findOne());
        assertTrue(c.getCount() == 0);

        for (int i=0; i < 100; i++) {
            c.insert(new BasicDBObject().append("i", i));
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
            c.insert(new BasicDBObject().append("i", i));
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
        c.save( BasicDBObjectBuilder.start().add( "a" , "eliot".getBytes() ).get() );
        
        DBObject out = c.findOne();
        byte[] b = (byte[])(out.get( "a" ) );
        assertEquals( "eliot" , new String( b ) );
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
                                     new BasicDBObject( "x" , 5.5 ) ) , false , false );
        assertEquals( Double.class , c.findOne().get( "x" ).getClass() );        
        
    }

    @Test
    public void testKeys1()
        throws MongoException {

        DBCollection c = _db.getCollection( "keys1" );
        c.drop();
        c.save( BasicDBObjectBuilder.start().push( "a" ).add( "x" , 1 ).get() );
        
        assertEquals( 1, ((DBObject)c.findOne().get("a")).get("x" ) );
        c.update( new BasicDBObject() , BasicDBObjectBuilder.start().push( "$set" ).add( "a.x" , 2 ).get() , false , false );
        assertEquals( 1 , c.find().count() );
        assertEquals( 2, ((DBObject)c.findOne().get("a")).get("x" ) );
        
    }

    @Test
    public void testTimestamp()
        throws MongoException {
        
        DBCollection c = _db.getCollection( "ts1" );
        c.drop();
        c.save( BasicDBObjectBuilder.start().add( "y" , new DBTimestamp() ).get() );
        
        DBTimestamp t = (DBTimestamp)c.findOne().get("y");
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
    
    final Mongo _db;

    public static void main( String args[] )
        throws Exception {
        JavaClientTest ct = new JavaClientTest();
        //ct.runConsole();
        ct.testStrictWrite();
    }
}
