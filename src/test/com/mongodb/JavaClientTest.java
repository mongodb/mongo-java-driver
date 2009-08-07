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
    
    final Mongo _db;

    public static void main( String args[] )
        throws Exception {
        (new JavaClientTest()).runConsole();
    }
}
