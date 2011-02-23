// PoolOutputBufferTest.java

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

import java.util.*;

import org.testng.annotations.Test;

import org.bson.io.*;
import org.testng.Assert;

public class PoolOutputBufferTest extends Assert {

    public PoolOutputBufferTest(){
        for ( int x = 8; x<(PoolOutputBuffer.BUF_SIZE*3); x*=2 ){
            StringBuilder buf = new StringBuilder();
            while ( buf.length() < x )
                buf.append( x );
            _data.add( buf.toString() );
        }
    }

    @Test
    public void testBasic1(){
        PoolOutputBuffer buf = new PoolOutputBuffer();
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
    public void testBig1(){
        PoolOutputBuffer a = new PoolOutputBuffer();
        StringBuilder b = new StringBuilder();
        for ( String x : _data ){
            a.write( x.getBytes() );
            b.append( x );
        }
        assertEquals( a.asString() , b.toString() );
    }
    
    List<String> _data = new ArrayList<String>();
    
}
