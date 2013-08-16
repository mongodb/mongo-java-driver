// BSONTest.java

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

package org.bson;

import static org.testng.Assert.assertNotEquals;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Vector;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.OutputBuffer;
import org.bson.types.CodeWScope;
import org.testng.Assert;
import org.testng.annotations.Test;

public class BSONTest extends Assert {


    public BSONTest(){
        for ( int x = 8; x<2048; x*=2 ){
            StringBuilder buf = new StringBuilder();
            while ( buf.length() < x )
                buf.append( x );
            _data.add( buf.toString() );
        }
    }

    
    void _test( BSONObject o , int size , String hash )
        throws IOException {
        BSONEncoder e = new BasicBSONEncoder();
        OutputBuffer buf = new BasicOutputBuffer();
        e.set( buf );
        e.putObject( o );
        assertEquals( size , buf.size() );
        assertEquals( hash , buf.md5() );
        e.done();
        
        BSONDecoder d = new BasicBSONDecoder();
        BSONCallback cb = new BasicBSONCallback();
        int s = d.decode( new ByteArrayInputStream( buf.toByteArray() ) , cb );
        assertEquals( size , s );

        OutputBuffer buf2 = new BasicOutputBuffer();
        e.set( buf2 );
        e.putObject( (BSONObject)cb.get() );
        assertEquals( size , buf2.size() );
        assertEquals( hash , buf2.md5() );        
        
    }
    
    @Test
    public void testBasic1()
        throws IOException {
//        BSONObject o = new BasicBSONObject();
        _test( new BasicBSONObject( "x" , true ) , 9 , "6fe24623e4efc5cf07f027f9c66b5456" );    

        _test( new BasicBSONObject( "x" , null ) , 8 , "12d43430ff6729af501faf0638e68888" );
        _test( new BasicBSONObject( "x" , 5.2 ) , 16 , "aaeeac4a58e9c30eec6b0b0319d0dff2" );
        _test( new BasicBSONObject( "x" , "eliot" ), 18 , "331a3b8b7cbbe0706c80acdb45d4ebbe" );
        _test( new BasicBSONObject( "x" , 5.2 ).append( "y" , "truth" ).append( "z" , 1.1 ) ,
               40 , "7c77b3a6e63e2f988ede92624409da58" );
        
        _test( new BasicBSONObject( "a" , new BasicBSONObject( "b" , 1.1 ) ) , 24 , "31887a4b9d55cd9f17752d6a8a45d51f" );
        _test( new BasicBSONObject( "x" , 5.2 ).append( "y" , new BasicBSONObject( "a" , "eliot" ).append( "b" , true ) ).append( "z" , null ) , 44 , "b3de8a0739ab329e7aea138d87235205" );
        _test( new BasicBSONObject( "x" , 5.2 ).append( "y" , new Object[]{ "a" , "eliot" , "b" , true } ).append( "z" , null ) , 62 , "cb7bad5697714ba0cbf51d113b6a0ee8" );
        
        _test( new BasicBSONObject( "x" , 4 ) , 12 , "d1ed8dbf79b78fa215e2ded74548d89d" );
    }

    @Test( expectedExceptions = IllegalArgumentException.class )
    public void testNullKeysFail() {
        BSONEncoder e = new BasicBSONEncoder();
        OutputBuffer buf = new BasicOutputBuffer();
        e.set( buf );
        e.putObject( new BasicBSONObject( "foo\0bar","baz" ) );
    }
    
    @Test
    public void testArray()
        throws IOException {
        _test( new BasicBSONObject( "x" , new int[]{ 1 , 2 , 3 , 4} ) , 41 , "e63397fe37de1349c50e1e4377a45e2d" );
    }

    @Test
    public void testOB1(){
        BasicOutputBuffer buf = new BasicOutputBuffer();
        buf.write( "eliot".getBytes() );
        assertEquals( 5 , buf.getPosition() );
        assertEquals( 5 , buf.size() );
        
        assertEquals( "eliot" , buf.asString() );

        buf.setPosition( 2 );
        buf.write( "z".getBytes() );
        assertEquals( "elzot" , buf.asString() );
        
        buf.seekEnd();
        buf.write( "foo".getBytes() );
        assertEquals( "elzotfoo" , buf.asString() );

        buf.seekStart();
        buf.write( "bar".getBytes() );
        assertEquals( "barotfoo" , buf.asString() );

    }

    @Test
    public void testCode()
      throws IOException{
        BSONObject scope = new BasicBSONObject( "x", 1 );
        CodeWScope c = new CodeWScope( "function() { x += 1; }" , scope );
        BSONObject code_object = new BasicBSONObject( "map" , c);
        _test( code_object , 53 , "52918d2367533165bfc617df50335cbb" );
    }

    @Test
    public void testBinary()
      throws IOException{
        byte[] data = new byte[10000];
        for(int i=0; i<10000; i++) {
          data[i] = 1;
        }
        BSONObject binary_object = new BasicBSONObject( "bin" , data);
        _test( binary_object , 10015 , "1d439ba5b959ecfe297a7862bf95bc10" );
    }

    @Test
    public void testOBBig1(){
        BasicOutputBuffer a = new BasicOutputBuffer();
        StringBuilder b = new StringBuilder();
        for ( String x : _data ){
            a.write( x.getBytes() );
            b.append( x );
        }
        assertEquals( a.asString() , b.toString() );
    }

    @Test
    public void testUTF8(){
        for ( int i=1; i<=Character.MAX_CODE_POINT; i++ ){
            
            if ( ! Character.isValidCodePoint( i ) )
                continue;
            
            String orig = new String( Character.toChars( i ) );
            BSONObject a = new BasicBSONObject( orig , orig );
            BSONObject b = BSON.decode( BSON.encode( a ) );
            assertEquals( a , b );
        }

    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCustomEncoders() 
      throws IOException{
        // If clearEncodingHooks isn't working the first test will fail.
        Transformer tf = new TestDateTransformer();
        BSON.addEncodingHook( TestDate.class, tf );
        BSON.clearEncodingHooks();
        TestDate td = new TestDate( 2009 , 01 , 23 , 10 , 53 , 42 );
        BSONObject o = new BasicBSONObject( "date" , td );
        BSONEncoder e = new BasicBSONEncoder();
        BSONDecoder d = new BasicBSONDecoder();
        BSONCallback cb = new BasicBSONCallback();
        OutputBuffer buf = new BasicOutputBuffer();
        e.set( buf );
        boolean encodeFailed = false;
        try {
            e.putObject( o );
        }
        catch ( IllegalArgumentException ieE ) {
            encodeFailed = true;
        }
        assertTrue( encodeFailed, "Expected encoding to fail but it didn't." );
        // Reset the buffer
        buf.seekStart();
        assertTrue( td instanceof TestDate );
        assertTrue( tf.transform( td ) instanceof java.util.Date, "Transforming a TestDate should yield a JDK Date" );

        BSON.addEncodingHook( TestDate.class, tf );
        e.putObject( o );
        e.done();

        d.decode( new ByteArrayInputStream( buf.toByteArray() ), cb );
        Object result = cb.get();
        assertTrue( result instanceof BSONObject, "Expected to retrieve a BSONObject but got '" + result.getClass() + "' instead." );
        BSONObject bson = (BSONObject) result;
        assertNotNull( bson.get( "date" ) );
        assertTrue( bson.get( "date" ) instanceof java.util.Date );

        // Check that the hooks registered
        assertNotNull( BSON.getEncodingHooks( TestDate.class ) );
        Vector expect = new Vector( 1 );
        expect.add( tf );
        assertEquals( BSON.getEncodingHooks( TestDate.class ), expect );
        assertTrue( BSON.getEncodingHooks( TestDate.class ).contains( tf ) );
        BSON.removeEncodingHook( TestDate.class, tf );
        assertFalse( BSON.getEncodingHooks( TestDate.class ).contains( tf ) );
    }

    @Test
    @SuppressWarnings({"deprecation", "unchecked"})
    public void testCustomDecoders() 
      throws IOException{
        // If clearDecodingHooks isn't working this whole test will fail.
        Transformer tf = new TestDateTransformer();
        BSON.addDecodingHook( Date.class, tf );
        BSON.clearDecodingHooks();
        TestDate td = new TestDate( 2009 , 01 , 23 , 10 , 53 , 42 );
        Date dt = new Date( 2009 , 01 , 23 , 10 , 53 , 42 );
        BSONObject o = new BasicBSONObject( "date" , dt );
        BSONDecoder d = new BasicBSONDecoder();
        BSONEncoder e = new BasicBSONEncoder();
        BSONCallback cb = new BasicBSONCallback();
        OutputBuffer buf = new BasicOutputBuffer();
        e.set( buf );
        e.putObject( o );
        e.done();

        d.decode( new ByteArrayInputStream( buf.toByteArray() ), cb );
        Object result = cb.get();
        assertTrue( result instanceof BSONObject, "Expected to retrieve a BSONObject but got '" + result.getClass() + "' instead." );
        BSONObject bson = (BSONObject) result;
        assertNotNull( bson.get( "date" ) );
        assertTrue( bson.get( "date" ) instanceof java.util.Date );

        BSON.addDecodingHook( Date.class, tf );

        d.decode( new ByteArrayInputStream( buf.toByteArray() ), cb );
        bson = (BSONObject) cb.get();
        assertNotNull( bson.get( "date" ) );
        assertTrue( bson.get( "date" ) instanceof TestDate );
        assertEquals( bson.get( "date" ), td );

        // Check that the hooks registered
        assertNotNull( BSON.getDecodingHooks( Date.class ) );
        Vector expect = new Vector( 1 );
        expect.add( tf );
        assertEquals( BSON.getDecodingHooks( Date.class ), expect );
        assertTrue( BSON.getDecodingHooks( Date.class ).contains( tf ) );
        BSON.removeDecodingHook( Date.class, tf );
        assertFalse( BSON.getDecodingHooks( Date.class ).contains( tf ) );

    }
    
    @Test
    public void testEquals() {
        assertNotEquals(new BasicBSONObject("a", 1111111111111111111L), new BasicBSONObject("a", 1111111111111111112L),
                "longs should not be equal");

        assertNotEquals(new BasicBSONObject("a", 100.1D), new BasicBSONObject("a", 100.2D),
                "doubles should not be equal");
        
        assertNotEquals(new BasicBSONObject("a", 100.1F), new BasicBSONObject("a", 100.2F),
                "floats should not be equal");
        
        assertEquals(new BasicBSONObject("a", 100.1D), new BasicBSONObject("a", 100.1D),
                "doubles should be equal");
        
        assertEquals(new BasicBSONObject("a", 100.1F), new BasicBSONObject("a", 100.1F),
                "floats should be equal");
        
        assertEquals(new BasicBSONObject("a", 100), new BasicBSONObject("a", 100L),
                "int and long should be equal");
    }

    private class TestDate {
        final int year;
        final int month;
        final int date;
        final int hour;
        final int minute;
        final int second;

        public TestDate(int year , int month , int date , int hour , int minute , int second) {
            this.year = year;
            this.month = month;
            this.date = date;
            this.hour = hour;
            this.minute = minute;
            this.second = second;
        }

        public TestDate(int year , int month , int date) {
            this( year , month , date , 0 , 0 , 0 );
        }

        @Override
        public boolean equals( Object other ){
            if ( this == other )
                return true;
            if ( !( other instanceof TestDate ) )
                return false;

            TestDate otherDt = (TestDate) other;
            return ( otherDt.year == this.year && otherDt.month == this.month && otherDt.date == this.date && otherDt.hour == this.hour
                    && otherDt.minute == this.minute && otherDt.second == this.second );
        }

        @Override
        public String toString(){
            return year + "-" + month + "-" + date + " " + hour + ":" + minute + ":" + second;
        }
    }

    private class TestDateTransformer implements Transformer {
        @SuppressWarnings( "deprecation" )
        public Object transform( Object o ){
            if ( o instanceof TestDate ) {
                TestDate td = (TestDate) o;
                return new java.util.Date( td.year , td.month , td.date , td.hour , td.minute , td.second );
            }
            else if ( o instanceof java.util.Date ) {
                Date d = (Date) o;
                return new TestDate( d.getYear() , d.getMonth() , d.getDate() , d.getHours() , d.getMinutes() , d.getSeconds() );
            }
            else
                return o;
        }
    }

    void _roundTrip( BSONObject o ){
        assertEquals( o , BSON.decode( BSON.encode( o ) ) );
    }

    @Test
    public void testRandomRoundTrips(){
        _roundTrip( new BasicBSONObject( "a" , "" ) );
        _roundTrip( new BasicBSONObject( "a" , "a" ) );
        _roundTrip( new BasicBSONObject( "a" , "b" ) );
    }

    List<String> _data = new ArrayList<String>();

}
