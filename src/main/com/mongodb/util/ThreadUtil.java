// ThreadUtil.java

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

package com.mongodb.util;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @deprecated This class is NOT a part of public API and will be dropped in 3.x versions.
 */
@Deprecated
public class ThreadUtil {

    /** Creates an prints a stack trace */
    public static void printStackTrace(){
        Exception e = new Exception();
        e.fillInStackTrace();
        e.printStackTrace();
    }

    /** Pauses for a given number of milliseconds
     * @param time number of milliseconds for which to pause
     */
    public static void sleep( long time ){
        try {
            Thread.sleep( time );
        }
        catch ( InterruptedException e ){
        }
    }

    public static void pushStatus( String what ){
        pushStatus( Thread.currentThread() , what );
    }
    
    public static void pushStatus( Thread t , String what ){
        getStatus( t ).push( what );
    }

    public static void clearStatus(){
        clearStatus( Thread.currentThread() );
    }

    public static void clearStatus( Thread t ){
        getStatus( t ).clear();
    }
    
    public static FastStack<String> getStatus(){
        return getStatus( Thread.currentThread() );
    }

    public static FastStack<String> getStatus( Thread t ){
        FastStack<String> s = _threads.get( t.getId() );
        if ( s == null ){
            s = new FastStack<String>();
            _threads.put( t.getId() , s );
        }
        return s;
    }

    private static final Map<Long,FastStack<String>> _threads = Collections.synchronizedMap( new HashMap<Long,FastStack<String>>() );
    
}
