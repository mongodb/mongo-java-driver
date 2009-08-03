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

import java.util.*;
import java.util.regex.*;
import java.io.IOException;

import org.testng.annotations.Test;

import com.mongodb.util.*;

public class ByteTest extends TestCase {

    public ByteTest()
        throws IOException , MongoException {
        super();
        _db = new Mongo( "127.0.0.1" , "bytetest" );
    }

    @Test(groups = {"basic"})
    public void testObject1(){
        ByteEncoder encoder = ByteEncoder.get();
        
        DBObject o = new BasicDBObject();
        o.put( "eliot" , "horowitz" );
        o.put( "num" , 517 );
        
        encoder.putObject( o );
        
        encoder.flip();
        
        ByteDecoder decoder = new ByteDecoder( encoder._buf );
        DBObject read = decoder.readObject();
        
        assertEquals( "horowitz" , read.get( "eliot" ).toString() );
        assertEquals( 517.0 , ((Integer)read.get( "num" )).doubleValue() );
        
        assertEquals( encoder._buf.limit() , encoder._buf.position() );
    }

    @Test(groups = {"basic"})
    public void testString()
        throws Exception {
        ByteEncoder encoder = ByteEncoder.get();
        
        String eliot = java.net.URLDecoder.decode( "horowitza%C3%BCa" , "UTF-8" );

        DBObject o = new BasicDBObject();
        o.put( "eliot" , eliot );
        o.put( "num" , 517 );
        
        encoder.putObject( o );
        
        encoder.flip();
        
        ByteDecoder decoder = new ByteDecoder( encoder._buf );
        DBObject read = decoder.readObject();
        
        assertEquals( eliot , read.get( "eliot" ).toString() );
        assertEquals( 517.0 , ((Integer)read.get( "num" )).doubleValue() );
        
        assertEquals( encoder._buf.limit() , encoder._buf.position() );
    }

    @Test(groups = {"basic"})
    public void testObject2(){
        ByteEncoder encoder = ByteEncoder.get();
        
        DBObject o = new BasicDBObject();
        o.put( "eliot" , "horowitz" );
        o.put( "num" , 517.3 );
        o.put( "z" , "y" );
        o.put( "asd" , null );
        
        DBObject o2 = new BasicDBObject();
        o2.put( "a" , "b" );
        o2.put( "b" , "a" );
        o.put( "next" , o2 );
        
        encoder.putObject( o );

        encoder.flip();
        
        ByteDecoder decoder = new ByteDecoder( encoder._buf );
        DBObject read = decoder.readObject();
        
        assertEquals( "horowitz" , read.get( "eliot" ).toString() );
        assertEquals( 517.3 , ((Double)read.get( "num" )).doubleValue() );
        assertEquals( "b" , ((DBObject)read.get( "next" ) ).get( "a" ).toString() );
        assertEquals( "a" , ((DBObject)read.get( "next" ) ).get( "b" ).toString() );
        assertEquals( "y" , read.get( "z" ).toString() );
        assertEquals( o.keySet().size() , read.keySet().size() );

        assertEquals( encoder._buf.limit() , encoder._buf.position() );
    }

    @Test(groups = {"basic"})
    public void testArray1(){
        ByteEncoder encoder = ByteEncoder.get();
        
        DBObject o = new BasicDBObject();
        o.put( "eliot" , "horowitz" );
        o.put( "num" , 517 );
        o.put( "z" , "y" );
        o.put( "asd" , null );
        o.put( "myt" , true );
        o.put( "myf" , false );
        
        List a = new ArrayList();
        a.add( "A" );
        a.add( "B" );
        a.add( "C" );
        o.put( "a" , a );

        o.put( "d" , new Date() );
        //o.put( "r" , Pattern.compile( "\\d+" , "i" ) );

        encoder.putObject( o );
        
        encoder.flip();
        
        ByteDecoder decoder = new ByteDecoder( encoder._buf );
        DBObject read = decoder.readObject();
        
        assertEquals( "horowitz" , read.get( "eliot" ).toString() );
        assertEquals( 517 , ((Integer)read.get( "num" )).intValue() );
        assertEquals( "y" , read.get( "z" ).toString() );
        assertEquals( o.keySet().size() , read.keySet().size() );
        assertEquals( 3 , a.size() );
        assertEquals( a.size() , ((List)read.get( "a" ) ).size() );
        assertEquals( "A" , ((List)read.get( "a" ) ).get( 0 ).toString() );
        assertEquals( "B" , ((List)read.get( "a" ) ).get( 1 ).toString() );
        assertEquals( "C" , ((List)read.get( "a" ) ).get( 2 ).toString() );
        assertEquals( ((Date)o.get("d")).getTime() , ((Date)read.get("d")).getTime() );
        assertEquals( true , (Boolean)o.get("myt") );
        assertEquals( false , (Boolean)o.get("myf") );
        //assertEquals( o.get( "r" ).toString() , read.get("r").toString() );

        assertEquals( encoder._buf.limit() , encoder._buf.position() );
    }

    @Test(groups = {"basic"})
    public void testObjcetId(){
        assertTrue( (new ObjectId()).compareTo( new ObjectId() ) < 0 );
        assertTrue( (new ObjectId(0 , 0 )).compareTo( new ObjectId() ) < 0 );
    }


    @Test(groups = {"basic"}) 
    public void testBinary() {
        byte barray[] = new byte[256];
        for( int i=0; i<256; i++ ) {
            barray[i] = (byte)(i-128);
        }

        DBObject o = new BasicDBObject();
        o.put( "bytes", barray );

        ByteEncoder encoder = ByteEncoder.get();
        int pos = encoder.putObject( o );
        assertEquals( pos, 277 );

        encoder.flip();
        ByteDecoder decoder = new ByteDecoder( encoder._buf );
        DBObject read = decoder.readObject();
        byte[] b = (byte[])read.get( "bytes" );
        for( int i=0; i<256; i++ ) {
            assertEquals( b[i], barray[i] );
        }
        assertEquals( o.keySet().size() , read.keySet().size() );
        assertEquals( encoder._buf.limit() , encoder._buf.position() );

        encoder.done();
        decoder.done();
    }

    private void go( DBObject o , int serialized_len ) {
        go( o, serialized_len, 0 );
    }

    private void go( DBObject o , int serialized_len, int transient_fields ) {
        ByteEncoder encoder = ByteEncoder.get();
        int pos = encoder.putObject( o );
        assertEquals( pos, serialized_len );

        encoder.flip();
        ByteDecoder decoder = new ByteDecoder( encoder._buf );
        DBObject read = decoder.readObject();
        assertEquals( o.keySet().size() - transient_fields, read.keySet().size() );
        assertEquals( encoder._buf.limit() , encoder._buf.position() );

        encoder.done();
        decoder.done();
    }

    @Test(groups = {"basic"})
    public void testEncodeDecode() {
        ArrayList t = new ArrayList();
        Object obj = null;

        // null object
        boolean threw = false;
        try {
            go( (DBObject)null, 0 );
        }
        catch( RuntimeException e ) {
            threw = true;
        }
        assertEquals( threw, true );
        threw = false;

        DBObject o = new BasicDBObject();
        int serialized_len = 5;

        // empty obj
        go( o, 5 );

        // _id == null
        o.put( "_id" , obj );
        assertEquals( Bytes.getType( obj ), Bytes.NULL );
        go( o, 10 );

        // _id == non-objid
        obj = new ArrayList();
        o.put( "_id" , obj );
        assertEquals( Bytes.getType( obj ), Bytes.ARRAY );
        go( o, 15 );

        // _id == ObjectId
        obj = new ObjectId();
        o.put( "_id" , obj );
        assertEquals( Bytes.getType( obj ), Bytes.OID );
        go( o, 22 );

        // dbcollection
        try {
            obj = _db.getCollection( "test" );
            o.put( "_id" , obj );
            assertEquals( Bytes.getType( obj ), 0 );
            go( o, 22 );
        }
        catch( RuntimeException e ) {
            threw = true;
        }
        assertEquals( threw, true );
        threw = false;

        t.add( "collection" );
        o = new BasicDBObject();
        o.put( "collection" , _db.getCollection( "test" ) );
        o.put( "_transientFields" , t );
        go( o, 5, 2 );
        t.clear();        

        // transientFields
        o = new BasicDBObject();
        o.put( "_transientFields", new ArrayList() );
        go( o, 5, 1 );

        t.add( "foo" );
        o = new BasicDBObject();
        o.put( "_transientFields", t );
        o.put( "foo", "bar" );
        go( o, 5, 2 );
        t.clear();

        // $where
        /*o = new BasicDBObject();
        o.put( "$where", "eval( true )" );
        go( o, 30 );
        */

        obj = 5;
        o = new BasicDBObject();
        o.put( "$where", obj );
        assertEquals( Bytes.getType( obj ), Bytes.NUMBER );
        go( o, 17 );
    }

    @Test(groups = {"basic"})
    public void testCameFromDB() {
        
        assertEquals( Bytes.cameFromDB( (DBObject)null ), false );
        DBObject o = new BasicDBObject();
        assertEquals( Bytes.cameFromDB( o ), false );
        o.put( "_id", new ObjectId() );
        assertEquals( Bytes.cameFromDB( o ), false );
        o.put( "_ns", "foo" );
        assertEquals( Bytes.cameFromDB( o ), true );
    }


    @Test(groups = {"basic"})
    public void testPatternFlags() {
        boolean threw = false;
        assertEquals( 0, Bytes.patternFlags( "" ) );
        assertEquals( "", Bytes.patternFlags( 0 ) );

        try {
            Bytes.patternFlags( "f" );
        }
        catch( RuntimeException e ) {
            threw = true;
        }
        assertEquals( threw, true );
        threw = false;

        try {
            Bytes.patternFlags( 513 );
        }
        catch( RuntimeException e ) {
            threw = true;
        }
        assertEquals( threw, true );

        Pattern lotsoflags = Pattern.compile( "foo", Pattern.CANON_EQ |
                                              Pattern.DOTALL |
                                              Pattern.CASE_INSENSITIVE |
                                              Pattern.UNIX_LINES |
                                              Pattern.MULTILINE |
                                              Pattern.LITERAL |
                                              Pattern.UNICODE_CASE |
                                              Pattern.COMMENTS |
                                              256 );

        String s = Bytes.patternFlags( lotsoflags.flags() );
        char prev = s.charAt( 0 );
        for( int i=1; i<s.length(); i++ ) {
            char current = s.charAt( i );
            assertTrue( prev < current );
            prev = current;
        }

        int check = Bytes.patternFlags( s );
        assertEquals( lotsoflags.flags(), check );
    }

    @Test(groups = {"basic"})
    public void testPattern() {
        Pattern p = Pattern.compile( "([a-zA-Z0-9_-])([a-zA-Z0-9_.-]*)@(((25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]|[0-9])\\.))" );
        DBObject o = new BasicDBObject();
        o.put( "p", p );

        ByteEncoder encoder = ByteEncoder.get();
        encoder.putObject( o );
        encoder.flip();

        ByteDecoder decoder = new ByteDecoder( encoder._buf );
        DBObject read = decoder.readObject();
        Pattern p2 = (Pattern)read.get( "p" );
        assertEquals( p2.pattern(), p.pattern() );
        assertEquals( o.keySet().size(), read.keySet().size() );
        assertEquals( encoder._buf.limit() , encoder._buf.position() );

        encoder.done();
        decoder.done();
    }

    @Test(groups = {"basic"})
    public void testLong() {
        long s = -9223372036854775808l;
        long m = 1l;
        long l = 9223372036854775807l;

        DBObject obj = BasicDBObjectBuilder.start().add("s", s).add("m", m).add("l", l).get();
        DBCollection c = _db.getCollection("test");
        c.drop();

        c.insert(obj);
        DBObject r = c.findOne();

        assertEquals(r.get("s"), -9223372036854775808l);
        assertEquals(r.get("m"), 1l);
        assertEquals(r.get("l"), 9223372036854775807l);
    }

    final DBBase _db;

    public static void main( String args[] )
        throws Exception {
        (new ByteTest()).runConsole();

    }

}
