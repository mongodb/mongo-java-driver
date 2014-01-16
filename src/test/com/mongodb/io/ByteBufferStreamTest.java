/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// ByteBufferStreamTest.java

package com.mongodb.io;

import com.mongodb.util.TestCase;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Random;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static org.junit.Assert.assertEquals;

public class ByteBufferStreamTest extends TestCase {

    @Test
    public void test1()
	throws IOException {
	_testInOut( 16 , 128 );
	_testInOut( 16 , 4 );
	_testInOut( 1024 , 128 );
	_testInOut( 1024 , 2048 );
    }
    
    private void _testInOut( int dataSize , int bufSize )
	throws IOException {
	String s = _getData( dataSize );
	byte[] bytes = s.getBytes();

	ByteBufferOutputStream bout = new ByteBufferOutputStream( bufSize );
	bout.write( bytes );
	
	assertEquals( (int)Math.ceil( (double)(bytes.length) / bufSize ) , bout.getBuffers().size() );

	ByteBufferInputStream bin = new ByteBufferInputStream( bout.getBuffers() , true );
	String out = new String( StreamUtil.readBytesFully( bin ) );
	assertEquals( s , out );

    }
    
    @Test
    public void testplay()
	throws IOException {
	_testplay( 16 , 128 );
	_testplay( 16 , 4 );
	_testplay( 1024 , 128 );
	_testplay( 1024 , 2048 );
	_testplay( 20000 , 200 );
    }
    
    private void _testplay( int dataSize , int bufSize )
	throws IOException {
	String s = _getData( dataSize );
	byte[] bytes = s.getBytes();
	
	ByteBufferOutputStream bout = new ByteBufferOutputStream( bufSize );
	bout.write( bytes );
	
	assertEquals( (int)Math.ceil( (double)(bytes.length) / bufSize ) , bout.getBuffers().size() );

	ByteBufferInputStream bin = new ByteBufferInputStream( bout.getBuffers() , true );
	ByteArrayInputStream arr = new ByteArrayInputStream( bytes );
	
	assertEquals( bin.available() , arr.available() );
	while ( arr.available() > 0 ){
	    assertEquals( bin.available() , arr.available() );
	    assertEquals( bin.read() , arr.read() );
	    assertEquals( bin.read( new byte[12] ) , arr.read( new byte[12] ) );
	}

	assertEquals( bin.available() , arr.available() );
    }


    @Test
    public void testZip1()
	throws IOException {
	_testZip( 128 , 2048 );
	_testZip( 1024 , 128 );
    }
    
    void _testZip( int dataSize , int bufSize )
	throws IOException {
	
	String s = _getData( dataSize );

	ByteBufferOutputStream bout = new ByteBufferOutputStream( bufSize );

	GZIPOutputStream gout = new GZIPOutputStream( bout );
	gout.write( s.getBytes() );
	gout.flush();
	gout.close();

	ByteBufferInputStream bin = new ByteBufferInputStream( bout.getBuffers() , true );
	GZIPInputStream gin = new GZIPInputStream( bin );
	String out = new String( StreamUtil.readBytesFully( gin ) );
	
	assertEquals( s , out );
    }
    
    String _getData( int size ){
	StringBuilder buf = new StringBuilder( size + 200 );
	while ( buf.length() < size )
	    buf.append( "eliot was here " + _rand.nextDouble() );
	return buf.toString();
    }

    static final Random _rand = new Random( 123123 );
}
