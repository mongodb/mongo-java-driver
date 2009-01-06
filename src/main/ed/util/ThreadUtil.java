// ThreadUtil.java

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

package ed.util;

import java.util.*;

/** @expose */
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
