// ByteTest.java

package ed.db;

import java.util.*;
import java.util.regex.*;
import java.io.IOException;

import org.testng.annotations.Test;

import ed.*;

public class ByteTest extends TestCase {
    public ByteTest()
        throws IOException {
        super();
        _db = new Mongo( "127.0.0.1" , "jtest" );        
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

    // ByteEncoder.get()
    @Test(groups = {"basic"})
    public void testEncoderGet() {
        int max = Math.min( Bytes.CONNECTIONS_PER_HOST, 2 * Bytes.BUFS_PER_50M );
        int count = 0;

        ArrayList<ByteEncoder> be = new ArrayList<ByteEncoder>();
        ByteEncoder b;
        while( (b = ByteEncoder.get()) != null && count < max ) {
            be.add( b );
            count++;
        }
        assertEquals( count, max );
        assertEquals( be.size(), max );

        for( ByteEncoder bee : be ) {
            bee.done();
        }
    }

    @Test
    public void testDecoderGet() {
        int max = 6 * Bytes.BUFS_PER_50M;
        int count = 0;

        ArrayList<ByteDecoder> be = new ArrayList<ByteDecoder>();
        ByteDecoder b;
        while( (b = ByteDecoder.get( _db , null ) ) != null && count < max ) {
            be.add( b );
            count++;
        }
        assertEquals( count, max );
        assertEquals( be.size(), max );

        for( ByteDecoder bd : be ) {
            bd.done();
        }
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
            Bytes.patternFlags( 6 );
        }
        catch( RuntimeException e ) {
            threw = true;
        }
        assertEquals( threw, true );
    }

    final Mongo _db;

    public static void main( String args[] )
        throws IOException {
        (new ByteTest()).runConsole();
    }

}
