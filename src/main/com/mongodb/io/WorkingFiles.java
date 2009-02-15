// WorkingFiles.java

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
