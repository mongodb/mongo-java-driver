// FileUtil.java

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
import java.util.zip.*;

public class FileUtil {

    public static void touch( File f )
        throws IOException {
        if ( f.createNewFile() )
            return;
        f.setLastModified( System.currentTimeMillis() );
    }
    
    public static String toString( File f ){
	return clean( f.toString() );
    }
    
    public static String clean( String s ){
	if ( s.contains( "\\" ) || s.contains( ":" ) ){
	    StringBuilder buf = new StringBuilder();
	    for ( int i=0; i<s.length(); i++ ){
		char c = s.charAt(i);
		if ( c == '\\' )
		    c = '/';
                else if ( c== ':' )
                    c = '_';
		buf.append( c );
	    }
	    s = buf.toString();
	}
	return s;
    }

    public static void deleteDirectory( File f ){

        if ( f.isDirectory() ){
            for ( File c : f.listFiles() )
                deleteDirectory( c );
        }
        
        f.delete();
    }

    public static void add( ZipOutputStream out , String path , File f )
        throws IOException {
        add( out , path , new FileInputStream( f ) , f.lastModified() );
    }

    /**
     * @param path - full path including filename
     */
    public static void add( ZipOutputStream out , String path , InputStream data , long lastModified )
        throws IOException {

        path = path.replace( '\\' , '/' );

        ZipEntry entry = new ZipEntry( path );
        entry.setTime( lastModified );
        
        out.putNextEntry( entry );
        StreamUtil.pipe( data , out );
    }
}
