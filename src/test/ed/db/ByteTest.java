// ByteTest.java

package ed.db;

import java.util.*;
import java.util.regex.*;

import org.testng.annotations.Test;

import ed.*;

public class ByteTest extends TestCase {

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

    public static void main( String args[] ){
        (new ByteTest()).runConsole();
    }

}
