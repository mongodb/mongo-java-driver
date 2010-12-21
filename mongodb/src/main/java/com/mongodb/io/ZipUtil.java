// ZipUtil.java

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

package com.mongodb.io;

import java.io.*;
import java.nio.*;
import java.util.*;
import java.util.zip.*;

public class ZipUtil {
    
    public static List<ByteBuffer> gzip( List<ByteBuffer> in , ByteBufferFactory factory ){
	if ( in == null || in.size() == 0 )
	    throw new IllegalArgumentException( "no data" );
	
	try {

	    ByteBufferOutputStream bout = new ByteBufferOutputStream( factory );
	    GZIPOutputStream gout = new GZIPOutputStream( bout );
	    
	    if ( in.get(0).hasArray() ){
		for ( ByteBuffer buf : in )
		    bout.write( buf.array() );
	    }
	    else {
		ByteBufferInputStream bin = new ByteBufferInputStream( in , false );
		StreamUtil.pipe( bin, gout );
	    }
	    
	    gout.flush();
	    gout.close();
	    
	    return bout.getBuffers( true );
	}
	catch ( IOException ioe ){
	    throw new RuntimeException( "should be impossible" , ioe );
	}
    }
    
}
