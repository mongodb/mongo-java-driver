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
import org.bson.BSON;
import org.bson.BSONDecoder;
import org.bson.BSONObject;
import org.bson.BasicBSONDecoder;
import org.bson.types.ObjectId;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.regex.Pattern;


@SuppressWarnings({"unchecked", "rawtypes"})
public class ByteTest extends TestCase {

    public ByteTest() throws IOException , MongoException {
        super();
        cleanupDB = "com_mongodb_unittest_ByteTest";
        _db = cleanupMongo.getDB( cleanupDB );
    }

    @Test(groups = {"basic"})
    public void testObject1(){
        DBObject o = new BasicDBObject();
        o.put( "eliot" , "horowitz" );
        o.put( "num" , 517 );
        
        
        BSONObject read = BSON.decode( BSON.encode( o ) );
        
        assertEquals( "horowitz" , read.get( "eliot" ).toString() );
        assertEquals( 517.0 , ((Integer)read.get( "num" )).doubleValue() );
    }

    @Test(groups = {"basic"})
    public void testString()
        throws Exception {
        
        String eliot = java.net.URLDecoder.decode( "horowitza%C3%BCa" , "UTF-8" );

        DBObject o = new BasicDBObject();
        o.put( "eliot" , eliot );
        o.put( "num" , 517 );
        
        BSONObject read = BSON.decode( BSON.encode( o ) );
        
        assertEquals( eliot , read.get( "eliot" ).toString() );
        assertEquals( 517.0 , ((Integer)read.get( "num" )).doubleValue() );

    }
    
    @Test(groups = {"basic"})
    public void testObject2(){
        DBObject o = new BasicDBObject();
        o.put( "eliot" , "horowitz" );
        o.put( "num" , 517.3 );
        o.put( "z" , "y" );
        o.put( "asd" , null );
        
        DBObject o2 = new BasicDBObject();
        o2.put( "a" , "b" );
        o2.put( "b" , "a" );
        o.put( "next" , o2 );
        
        BSONObject read = BSON.decode( BSON.encode( o ) );
        
        assertEquals( "horowitz" , read.get( "eliot" ).toString() );
        assertEquals( 517.3 , ((Double)read.get( "num" )).doubleValue() );
        assertEquals( "b" , ((BSONObject)read.get( "next" ) ).get( "a" ).toString() );
        assertEquals( "a" , ((BSONObject)read.get( "next" ) ).get( "b" ).toString() );
        assertEquals( "y" , read.get( "z" ).toString() );
        assertEquals( o.keySet().size() , read.keySet().size() );

    }

    @Test(groups = {"basic"})
    public void testArray1(){
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

        BSONObject read = BSON.decode( BSON.encode( o ) );
        
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

    }

    @Test
    public void testArray2(){
        DBObject x = new BasicDBObject();
        x.put( "a" , new String[]{ "a" , "b" , "c" } );
        x.put( "b" , new int[]{ 1 , 2 , 3 } );
        
        BSONObject y = BSON.decode( BSON.encode( x ) );
        
        List l = (List)y.get("a");
        assertEquals( 3 , l.size() );
        assertEquals( "a" , l.get(0) );
        assertEquals( "b" , l.get(1) );
        assertEquals( "c" , l.get(2) );

        l = (List)y.get("b");
        assertEquals( 3 , l.size() );
        assertEquals( 1 , l.get(0) );
        assertEquals( 2 , l.get(1) );
        assertEquals( 3 , l.get(2) );
    }

    @Test
    public void testCharacterEncode(){
        DBObject x = new BasicDBObject();
        x.put( "a" , new Character[]{ 'a' , 'b' , 'c' } );
        x.put( "b" , 's');

        BSONObject y = BSON.decode( BSON.encode( x ) );

        List l = (List)y.get("a");
        assertEquals( 3 , l.size() );
        assertEquals( "a" , l.get(0) );
        assertEquals( "b" , l.get(1) );
        assertEquals( "c" , l.get(2) );

        assertEquals( "s" , y.get("b") );
    }

    @Test(groups = {"basic"})
    public void testObjcetId(){
        assertTrue( (new ObjectId()).compareTo( new ObjectId() ) < 0 );
        assertTrue( (new ObjectId(0 , 0 , 0 )).compareTo( new ObjectId() ) < 0 );
        assertTrue( (new ObjectId(0 , 0 , 0 )).compareTo( new ObjectId( 0 , 0 , 1 ) ) < 0 );

        assertTrue( (new ObjectId(5 , 5 , 5 )).compareTo( new ObjectId( 5 , 5 , 6 ) ) < 0 );
        assertTrue( (new ObjectId(5 , 5 , 5 )).compareTo( new ObjectId( 5 , 6 , 5 ) ) < 0 );
        assertTrue( (new ObjectId(5 , 5 , 5 )).compareTo( new ObjectId( 6 , 5 , 5 ) ) < 0 );

    }


    @Test(groups = {"basic"}) 
    public void testBinary() {
        byte barray[] = new byte[256];
        for( int i=0; i<256; i++ ) {
            barray[i] = (byte)(i-128);
        }

        DBObject o = new BasicDBObject();
        o.put( "bytes", barray );

        byte[] encoded = BSON.encode( o );
        assertEquals( 273 , encoded.length );

        BSONObject read = BSON.decode( encoded );
        byte[] b = (byte[])read.get( "bytes" );
        
        assertEquals(barray.length, b.length);
        for( int i=0; i<256; i++ ) {
            assertEquals( b[i], barray[i] );
        }
        assertEquals( o.keySet().size() , read.keySet().size() );
    }

    private void go( DBObject o , int serialized_len ) {
        go( o, serialized_len, 0 );
    }

    private void go( DBObject o , int serialized_len, int transient_fields ) {
        byte[] encoded = BSON.encode( o );
        assertEquals( serialized_len , encoded.length );

        BSONObject read = BSON.decode( encoded );
        assertEquals( o.keySet().size() - transient_fields, read.keySet().size() );
        if ( transient_fields == 0 )
            assertEquals( o , read );
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
        
        o = new BasicDBObject();
        o.put( "z" , "" );
        go( o, 13 );
        t.clear();

        // $where
        /*o = new BasicDBObject();
        o.put( "$where", "eval( true )" );
        go( o, 30 );
        */

        obj = 5;
        o = new BasicDBObject();
        o.put( "$where", obj );
        assertEquals( Bytes.getType( obj ), Bytes.NUMBER_INT );
        go( o, 17 );
    }

    @Test(groups = {"basic"})
    public void testPatternFlags() {
        boolean threw = false;
        assertEquals( 0, Bytes.regexFlags( "" ) );
        assertEquals( "", Bytes.regexFlags( 0 ) );

        try {
            Bytes.regexFlags( "f" );
        }
        catch( RuntimeException e ) {
            threw = true;
        }
        assertEquals( threw, true );
        threw = false;

        try {
            Bytes.regexFlags( 513 );
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

        String s = Bytes.regexFlags( lotsoflags.flags() );
        char prev = s.charAt( 0 );
        for( int i=1; i<s.length(); i++ ) {
            char current = s.charAt( i );
            assertTrue( prev < current );
            prev = current;
        }

        int check = Bytes.regexFlags( s );
        assertEquals( lotsoflags.flags(), check );
    }

    @Test(groups = {"basic"})
    public void testPattern() {
        Pattern p = Pattern.compile( "([a-zA-Z0-9_-])([a-zA-Z0-9_.-]*)@(((25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]|[0-9])\\.))" );
        DBObject o = new BasicDBObject();
        o.put( "p", p );

        BSONObject read = BSON.decode( BSON.encode( o ) );
        Pattern p2 = (Pattern)read.get( "p" );
        assertEquals( p2.pattern(), p.pattern() );
        assertEquals( o.keySet().size(), read.keySet().size() );

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

    @Test(groups = {"basic"})
    public void testIdOrder() {
        
        DBCollection c = _db.getCollection( "testidorder" );
        c.drop();
        
        BasicDBObject x = new BasicDBObject();
        x.put( "a" , 5 );
        x.put( "_id" , 6 );
        BasicDBObject y = new BasicDBObject();
        y.put( "b" , 7 );
        y.put( "_id" , 8 );
        x.put( "c" , y );
        
        c.insert( x );
        
        DBObject out = c.findOne();
        _testKeys( new String[]{ "_id" , "a" , "c" } , out.keySet() );
        _testKeys( new String[]{ "b" , "_id" } , ((DBObject)out.get( "c" )).keySet() );
    }


    void _testKeys( String[] want , Set<String> got ){
        assertEquals( want.length , got.size() );
        int pos = 0;
        for ( String s : got ){
            assertEquals( want[pos++] , s );
        }
    }

    @Test(groups = {"basic"})
    public void testBytes2(){
        DBObject x = BasicDBObjectBuilder.start( "x" , 1 ).add( "y" , "asdasd" ).get();
        byte[] b = Bytes.encode( x );
        assertEquals( x , Bytes.decode( b ) );
    }

    @Test
    public void testMany()
        throws IOException {

        DBObject orig = new BasicDBObject();
        orig.put( "a" , 5 );
        orig.put( "ab" , 5.1 );
        orig.put( "abc" , 5123L );
        orig.put( "abcd" , "asdasdasd" );
        orig.put( "abcde" , "asdasdasdasdasdasdasdasd" );
        orig.put( "abcdef" , Arrays.asList( new String[]{ "asdasdasdasdasdasdasdasd" , "asdasdasdasdasdasdasdasd" } ) );
        
        byte[] b = Bytes.encode( orig );
        final int n = 1000;
        
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for ( int i=0; i<n; i++ )
            out.write( b );
        
        ByteArrayInputStream in = new ByteArrayInputStream( out.toByteArray() );
        BSONDecoder d = new BasicBSONDecoder();
        for ( int i=0; i<n; i++ ){
            BSONObject x = d.readObject( in );
            assertEquals( orig , x );
        }
        assertEquals( -1 , in.read() );
    }

    int _fix( int x ){
        if ( x < 0 )
            return -1;
        if ( x > 0 ) 
            return 1;
        return 0;
    }

    @Test
    public void testObjcetIdCompare(){
        Random r = new Random( 171717 );
        
        List<ObjectId> l = new ArrayList<ObjectId>();
        for ( int i=0; i<10000; i++ ){
            l.add( new ObjectId( new Date( Math.abs( r.nextLong() ) ) , Math.abs( r.nextInt() ) , Math.abs( r.nextInt() ) ) );
        }

        for ( int i=1; i<l.size(); i++ ){
            int a = _fix( l.get(0).compareTo( l.get(i) ) );
            int b = _fix( l.get(0).toString().compareTo( l.get(i).toString() ) );
            if ( a == b )
                continue;
            throw new RuntimeException( "broken [" + l.get(0) + "] [" + l.get(i) + "] a: " + a + " b: " + b );
        }

        DBCollection c = _db.getCollection( "testObjcetIdCompare" );
        c.drop();
        
        for ( ObjectId o : l ){
            c.insert( new BasicDBObject( "_id" , o ) );
        }

        Collections.sort( l );
        
        List<DBObject> out = c.find().sort( new BasicDBObject( "_id" , 1 ) ).toArray();
        
        assertEquals( l.size() , out.size() );
        
        for ( int i=0; i<l.size(); i++ ){
            assertEquals( l.get(i) , out.get(i).get( "_id" ) );
        }
        
            
    }

    final DB _db;

    public static void main( String args[] )
        throws Exception {
        (new ByteTest()).runConsole();
        
    }

}
