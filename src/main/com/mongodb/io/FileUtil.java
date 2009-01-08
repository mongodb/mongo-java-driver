// FileUtil.java

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
