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
import java.nio.*;
import java.util.*;
import java.util.zip.*;

import com.mongodb.*;
import com.mongodb.util.*;

import org.testng.annotations.Test;

import org.bson.io.*;

public class BSONTest extends TestCase {


    public BSONTest(){
        for ( int x = 8; x<2048; x*=2 ){
            StringBuilder buf = new StringBuilder();
            while ( buf.length() < x )
                buf.append( x );
            _data.add( buf.toString() );
        }
    }

    
    void _test( BSONObject o , int size , String hash ){
        BSONEncoder e = new BSONEncoder();
        OutputBuffer buf = new BasicOutputBuffer();
        e.set( buf );
        e.putObject( o );
        assertEquals( size , buf.size() );
        assertEquals( hash , buf.md5() );
    }

    @Test
    public void testBasic1(){
        BSONObject o = new BasicBSONObject();

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
    public void testOBBig1(){
        BasicOutputBuffer a = new BasicOutputBuffer();
        StringBuilder b = new StringBuilder();
        for ( String x : _data ){
            a.write( x.getBytes() );
            b.append( x );
        }
        assertEquals( a.asString() , b.toString() );
    }
    
    List<String> _data = new ArrayList<String>();


    public static void main( String args[] ){
        (new BSONTest()).runConsole();
    }

}
