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

import java.io.*;
import java.util.*;

import org.bson.io.*;
import org.bson.types.*;
import org.testng.annotations.*;

import com.mongodb.util.*;

public class BSONTest extends TestCase {


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
        BSONEncoder e = new BSONEncoder();
        OutputBuffer buf = new BasicOutputBuffer();
        e.set( buf );
        e.putObject( o );
        assertEquals( size , buf.size() );
        assertEquals( hash , buf.md5() );
        e.done();
        
        BSONDecoder d = new BSONDecoder();
        BasicBSONCallback cb = new BasicBSONCallback();
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
        _test( binary_object , 10019 , "682d9a636619b135fa9801ac42c48a10" );
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
    
    List<String> _data = new ArrayList<String>();


    public static void main( String args[] ){
        (new BSONTest()).runConsole();
    }

}
