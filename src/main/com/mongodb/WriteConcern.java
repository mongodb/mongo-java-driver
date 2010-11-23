// WriteConcern.java

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

package com.mongodb;

import java.lang.reflect.*;
import java.util.*;

/**
 * <p>WriteConcern control the write behavior for with various options, as well as exception raising on error conditions.</p>
 * 
 * <p>
 * <b>w</b>
 * <ul>
 * 	<li>-1 = don't even report network errors </li>
 *  <li> 0 = default, don't call getLastError by default </li>
 *  <li> 1 = basic, call getLastError, but don't wait for slaves</li>
 *  <li> 2+= wait for slaves </li>
 * </ul>
 * <b>wtimeout</b> how long to wait for slaves before failing
 * <ul>
 *   <li>0 = indefinite </li>
 *   <li>> 0 = ms to wait </li>
 * </ul>
 * </p>
 * <p><b>fsync</b> force fsync to disk </p>
 * 
 * @dochub databases
 */
public class WriteConcern {

    /** No exceptions are raised, even for network issues */
    public final static WriteConcern NONE = new WriteConcern(-1);

    /** Exceptions are raised for network issues, but not server errors */
    public final static WriteConcern NORMAL = new WriteConcern(0);
    
    /** Exceptions are raised for network issues, and server errors; waits on a server for the write operation */
    public final static WriteConcern SAFE = new WriteConcern(1);
    
    /** Exceptions are raised for network issues, and server errors and the write operation waits for the server to flush the data to disk*/
    public final static WriteConcern FSYNC_SAFE = new WriteConcern(true);

    /** Exceptions are raised for network issues, and server errors; waits for at least 2 servers for the write operation*/
    public final static WriteConcern REPLICAS_SAFE = new WriteConcern(2);
    
    // map of the constants from above for use by fromString
    private static Map<String, WriteConcern> _namedConcerns = null;
    
    /**
     * Get the WriteConcern constants by name: NONE, NORMAL, SAFE, FSYNC_SAFE,
     * REPLICA_SAFE. (matching is done case insensitively)
     */
    public static WriteConcern valueOf(String name) {
        if (_namedConcerns == null) {
            HashMap<String, WriteConcern> newMap = new HashMap<String, WriteConcern>( 8 , 1 );
            for (Field f : WriteConcern.class.getFields())
                if (Modifier.isStatic( f.getModifiers() ) && f.getType().equals( WriteConcern.class )) {
                    try {
                        newMap.put( f.getName().toLowerCase(), (WriteConcern) f.get( null ) );
                    } catch (Exception e) {
                        throw new RuntimeException( e );
                    }
                }
            
            // Thought about doing a synchronize but this seems just as safe and
            // I don't care about race conditions.
            _namedConcerns = newMap;
        }
        
        return _namedConcerns.get( name.toLowerCase() );
    }

    public WriteConcern(){
        this(0);
    }
    
    /** <p>Specifies the number of servers to wait for on the write operation, and exception raising behavior </p>
     *	<p> w represents # of servers:
     * 		<ul>
     * 			<li>{@code w=-1} None, no checking is done</li>
     * 			<li>{@code w=0} None, network socket errors raised</li>
     * 			<li>{@code w=1} Checks server for errors as well as network socket errors raised</li>
     * 			<li>{@code w>1} Checks servers (w) for errors as well as network socket errors raised</li>
     * 		</ul>
     * 	</p>
     * 
     **/
    public WriteConcern( int w ){
        this( w , 0 , false );
    }
    
    /** <p>Specifies the number of servers to wait for on the write operation, and the amount of time (ms) to wait.</p>
     *	<p> Note: w should be > 1 </p>
     * 
     **/
    public WriteConcern( int w , int wtimeout ){
        this( w , wtimeout , false );
    }

    public WriteConcern( boolean fsync ){
        this( 1 , 0 , fsync);
    }
    
    /** <p>Specifies the number of servers to wait for on the write operation, and the amount of time (ms) to wait.</p>
     *	<p> Note: w should be > 1 </p>
     **/
    public WriteConcern( int w , int wtimeout , boolean fsync ){
        _w = w;
        _wtimeout = wtimeout;
        _fsync = fsync;
        
        _command = new BasicDBObject( "getlasterror" , 1 );
        if ( _w > 0 ){
            _command.put( "w" , _w );
            _command.put( "wtimeout" , wtimeout );
        }

        if ( _fsync )
            _command.put( "fsync" , true );
    }

    public BasicDBObject getCommand(){
        return _command;
    }

    /** @return the number of servers to write to */
    public int getW(){
        return _w;
    }

    /** @return the write timeout (in milliseconds) */
    public int getWtimeout(){
        return _wtimeout;
    }

    /** @return If files are sync'd to disk. */
    public boolean fsync(){
        return _fsync;
    }

    public boolean raiseNetworkErrors(){
        return _w >= 0;
    }

    public boolean callGetLastError(){
        return _w > 0;
    }

    @Override
    public String toString(){
        return "WriteConcern " + _command;
    }

    final int _w; 
    final int _wtimeout;
    final boolean _fsync;

    final BasicDBObject _command;

}
