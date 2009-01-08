// ZipUtil.java

/**
*    Copyright (C) 2008 10gen Inc.
*  
*    This program is free software: you can redistribute it and/or  modify
*    it under the terms of the GNU Affero General Public License, version 3,
*    as published by the Free Software Foundation.
*  
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU Affero General Public License for more details.
*  
*    You should have received a copy of the GNU Affero General Public License
*    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
