// WorkingFiles.java

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
import java.util.*;

public class WorkingFiles {
    
    private static final String APP_NAME;
    static {
        String className = "none";
        
        for ( Map.Entry<Thread,StackTraceElement[]> e : Thread.getAllStackTraces().entrySet() ){
            StackTraceElement[] stack = e.getValue();
            if ( stack == null || stack.length == 0 )
                continue;
            
            StackTraceElement last = stack[stack.length-1];
            if ( "main".equals( last.getMethodName() ) )
                className = last.getClassName().replaceAll( "^.*\\." , "" );
        }

        APP_NAME = className;
    }
    private static final String TMP_DIR = "/tmp/jxp-" + System.getProperty( "user.name" ) + "-" + APP_NAME + "/";
    private static final File TMP_FILE = new File( TMP_DIR );
    static {
        TMP_FILE.mkdirs();
    }

    public static final String getTmpDir(){
        return TMP_DIR;
    }

    public static File getTypeDir( String type ){
        File f = new File( TMP_FILE , type );
        f.mkdirs();
        return f;
    }

    public static File getTMPFile( String type , String name ){
        name = FileUtil.clean( name );

        while ( name.startsWith( "/" ) )
            name = name.substring(1);
        
        File f = new File( getTypeDir( type ) , name );
        
        if ( name.contains( "/" ) )
            f.getParentFile().mkdirs();

        return f;
    }
    
}
