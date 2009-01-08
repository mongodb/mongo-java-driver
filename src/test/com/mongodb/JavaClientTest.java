// JavaClientTest.java

package com.mongodb;

import java.io.*;
import java.util.*;

import org.testng.annotations.Test;

import com.mongodb.util.*;

public class JavaClientTest extends TestCase {
    
    public JavaClientTest()
        throws IOException {
        _db = new Mongo( "127.0.0.1" , "jtest" );        
    }

    @Test
    public void test1(){
        DBCollection c = _db.getCollection( "test1" );;
        c.drop();

        DBObject m = new BasicDBObject();
        m.put( "name" , "eliot" );
        m.put( "state" , "ny" );
        
        c.save( m );
        
        assert( m.containsKey( "_id" ) );

        Map out = (Map)(c.find( m.get( "_id" ).toString() ));
        assertEquals( "eliot" , out.get( "name" ) );
        assertEquals( "ny" , out.get( "state" ) );
    }

    @Test
    public void test2(){
        DBCollection c = _db.getCollection( "test2" );;
        c.drop();
        
        DBObject m = new BasicDBObject();
        m.put( "name" , "eliot" );
        m.put( "state" , "ny" );
        
        Map<String,Object> sub = new HashMap<String,Object>();
        sub.put( "bar" , "1z" );
        m.put( "foo" , sub );

        c.save( m );
        
        assert( m.containsKey( "_id" ) );

        Map out = (Map)(c.find( m.get( "_id" ).toString() ));
        assertEquals( "eliot" , out.get( "name" ) );
        assertEquals( "ny" , out.get( "state" ) );

        Map z = (Map)out.get( "foo" );
        assertNotNull( z );
        assertEquals( "1z" , z.get( "bar" ) );
    }

    @Test
    public void testWhere1(){
        DBCollection c = _db.getCollection( "testWhere1" );
        c.drop();
        assertNull( c.findOne() );
        
        c.save( BasicDBObjectBuilder.start().add( "a" , 1 ).get() );
        assertNotNull( c.findOne() != null );
     
        assertNotNull( c.findOne( BasicDBObjectBuilder.start().add( "$where" , "this.a == 1" ).get() ) );
        assertNull( c.findOne( BasicDBObjectBuilder.start().add( "$where" , "this.a == 2" ).get() ) );
    }

    @Test
    public void testBinary(){
        DBCollection c = _db.getCollection( "testBinary" );
        c.save( BasicDBObjectBuilder.start().add( "a" , "eliot".getBytes() ).get() );
        
        DBObject out = c.findOne();
        byte[] b = (byte[])(out.get( "a" ) );
        assertEquals( "eliot" , new String( b ) );
    }
    
    final Mongo _db;

    public static void main( String args[] )
        throws Exception {
        (new JavaClientTest()).runConsole();
    }
}
