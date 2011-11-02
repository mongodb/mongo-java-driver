// WriteConcern.java

/**
 *      Copyright (C) 2008-2011 10gen Inc.
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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

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

    /** Exceptions are raised for network issues, and server errors; waits on a majority of servers for the write operation */
    public final static WriteConcern MAJORITY = new Majority();

    /** Exceptions are raised for network issues, and server errors; the write operation waits for the server to flush the data to disk*/
    public final static WriteConcern FSYNC_SAFE = new WriteConcern(true);

    /** Exceptions are raised for network issues, and server errors; the write operation waits for the server to group commit to the journal file on disk*/
    public final static WriteConcern JOURNAL_SAFE = new WriteConcern( 1, 0, false, true );

    /** Exceptions are raised for network issues, and server errors; waits for at least 2 servers for the write operation*/
    public final static WriteConcern REPLICAS_SAFE = new WriteConcern(2);

    // map of the constants from above for use by fromString
    private static Map<String, WriteConcern> _namedConcerns = null;

    /**
     * Default constructor keeping all options as default
     */
    public WriteConcern(){
        this(0);
    }

    /**
     * Calls {@link WriteConcern#WriteConcern(int, int, boolean)} with wtimeout=0 and fsync=false
     * @param w number of writes
     */
    public WriteConcern( int w ){
        this( w , 0 , false );
    }

    /**
     * Tag based Write Concern with wtimeout=0, fsync=false, and j=false
     * @param w Write Concern tag
     */
    public WriteConcern( String w ){
        this( w , 0 , false, false );
    }

    /**
     * Calls {@link WriteConcern#WriteConcern(int, int, boolean)} with fsync=false
     * @param w number of writes
     * @param wtimeout timeout for write operation
     */
    public WriteConcern( int w , int wtimeout ){
        this( w , wtimeout , false );
    }

    /**
     * Calls {@link WriteConcern#WriteConcern(int, int, boolean)} with w=1 and wtimeout=0
     * @param fsync whether or not to fsync
     */
    public WriteConcern( boolean fsync ){
        this( 1 , 0 , fsync);
    }

    /**
     * Creates a WriteConcern object.
     * <p>Specifies the number of servers to wait for on the write operation, and exception raising behavior </p>
     *	<p> w represents the number of servers:
     * 		<ul>
     * 			<li>{@code w=-1} None, no checking is done</li>
     * 			<li>{@code w=0} None, network socket errors raised</li>
     * 			<li>{@code w=1} Checks server for errors as well as network socket errors raised</li>
     * 			<li>{@code w>1} Checks servers (w) for errors as well as network socket errors raised</li>
     * 		</ul>
     * 	</p>
     * @param w number of writes
     * @param wtimeout timeout for write operation
     * @param fsync whether or not to fsync
     */
    public WriteConcern( int w , int wtimeout , boolean fsync ){
        this(w, wtimeout, fsync, false);
    }

    /**
     * Creates a WriteConcern object.
     * <p>Specifies the number of servers to wait for on the write operation, and exception raising behavior </p>
     *	<p> w represents the number of servers:
     * 		<ul>
     * 			<li>{@code w=-1} None, no checking is done</li>
     * 			<li>{@code w=0} None, network socket errors raised</li>
     * 			<li>{@code w=1} Checks server for errors as well as network socket errors raised</li>
     * 			<li>{@code w>1} Checks servers (w) for errors as well as network socket errors raised</li>
     * 		</ul>
     * 	</p>
     * @param w number of writes
     * @param wtimeout timeout for write operation
     * @param fsync whether or not to fsync
     * @param j whether writes should wait for a journaling group commit
     */
    public WriteConcern( int w , int wtimeout , boolean fsync , boolean j ){
        this( w, wtimeout, fsync, j, false);
    }

    /**
     * Creates a WriteConcern object.
     * <p>Specifies the number of servers to wait for on the write operation, and exception raising behavior </p>
     *	<p> w represents the number of servers:
     * 		<ul>
     * 			<li>{@code w=-1} None, no checking is done</li>
     * 			<li>{@code w=0} None, network socket errors raised</li>
     * 			<li>{@code w=1} Checks server for errors as well as network socket errors raised</li>
     * 			<li>{@code w>1} Checks servers (w) for errors as well as network socket errors raised</li>
     * 		</ul>
     * 	</p>
     * @param w number of writes
     * @param wtimeout timeout for write operation
     * @param fsync whether or not to fsync
     * @param j whether writes should wait for a journaling group commit
     * @param continueOnInsertError if batch inserts should continue after the first error
     */
    public WriteConcern( int w , int wtimeout , boolean fsync , boolean j, boolean continueOnInsertError){
        _w = w;
        _wtimeout = wtimeout;
        _fsync = fsync;
        _j = j;
        _continueOnErrorForInsert = continueOnInsertError;
    }

    /**
     * Creates a WriteConcern object.
     * <p>Specifies the number of servers to wait for on the write operation, and exception raising behavior </p>
     *	<p> w represents the number of servers:
     * 		<ul>
     * 			<li>{@code w=-1} None, no checking is done</li>
     * 			<li>{@code w=0} None, network socket errors raised</li>
     * 			<li>{@code w=1} Checks server for errors as well as network socket errors raised</li>
     * 			<li>{@code w>1} Checks servers (w) for errors as well as network socket errors raised</li>
     * 		</ul>
     * 	</p>
     * @param w number of writes
     * @param wtimeout timeout for write operation
     * @param fsync whether or not to fsync
     * @param j whether writes should wait for a journaling group commit
     */
    public WriteConcern( String w , int wtimeout , boolean fsync, boolean j ){
        this( w, wtimeout, fsync, j, false);
    }

    /**
     * Creates a WriteConcern object.
     * <p>Specifies the number of servers to wait for on the write operation, and exception raising behavior </p>
     *	<p> w represents the number of servers:
     * 		<ul>
     * 			<li>{@code w=-1} None, no checking is done</li>
     * 			<li>{@code w=0} None, network socket errors raised</li>
     * 			<li>{@code w=1} Checks server for errors as well as network socket errors raised</li>
     * 			<li>{@code w>1} Checks servers (w) for errors as well as network socket errors raised</li>
     * 		</ul>
     * 	</p>
     * @param w number of writes
     * @param wtimeout timeout for write operation
     * @param fsync whether or not to fsync
     * @param j whether writes should wait for a journaling group commit
     * @param continueOnInsertError if batch inserts should continue after the first error
     * @return
     */
    public WriteConcern( String w , int wtimeout , boolean fsync, boolean j, boolean continueOnInsertError ){
        _w = w;
        _wtimeout = wtimeout;
        _fsync = fsync;
        _j = j;
        _continueOnErrorForInsert = continueOnInsertError;
    }

    public BasicDBObject getCommand(){
        BasicDBObject _command = new BasicDBObject( "getlasterror" , 1 );

        if ( _w instanceof Integer && ( (Integer) _w > 0) ||
            ( _w instanceof String && _w != null ) ){
            _command.put( "w" , _w );
            _command.put( "wtimeout" , _wtimeout );
        }

        if ( _fsync )
            _command.put( "fsync" , true );

        if ( _j )
            _command.put( "j", true );

        return _command;
    }

    /**
     * Sets the w value (the write strategy)
     * @param wValue
     */
    public void setWObject(Object w) {
        if ( ! (w instanceof Integer) && ! (w instanceof String) )
            throw new IllegalArgumentException("The w parameter must be an int or a String");
        this._w = w;
    }

    /**
     * Gets the w value (the write strategy)
     * @return
     */
    public Object getWObject(){
        return _w;
    }

    /**
     * Gets the w parameter (the write strategy)
     * @return
     */
    public int getW(){
        return (Integer) _w;
    }

    /**
     * Gets the w parameter (the write strategy) in String format
     * @return
     */
    public String getWString(){
        return _w.toString();
    }

    /**
     * Gets the write timeout (in milliseconds)
     * @return
     */
    public int getWtimeout(){
        return _wtimeout;
    }

    /**
     * Gets the fsync flag (fsync to disk on the server)
     * @return
     */
    public boolean getFsync(){
        return _fsync;
    }

    /**
     * Gets the fsync flag (fsync to disk on the server)
     * @return
     */
    public boolean fsync(){
        return _fsync;
    }

    /**
     * Returns whether network error may be raised (w >= 0)
     * @return
     */
    public boolean raiseNetworkErrors(){
        if (_w instanceof Integer)
            return (Integer) _w >= 0;
        return _w != null;
    }

    /**
     * Returns whether "getlasterror" should be called (w > 0)
     * @return
     */
    public boolean callGetLastError(){
        if (_w instanceof Integer)
            return (Integer) _w  > 0;
        return _w != null;
    }

    /**
     * Gets the WriteConcern constants by name: NONE, NORMAL, SAFE, FSYNC_SAFE,
     * REPLICA_SAFE. (matching is done case insensitively)
     * @param name
     * @return
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

    @Override
    public String toString(){
        return "WriteConcern " + getCommand() + " / (Continue Inserting on Errors? " + getContinueOnErrorForInsert() + ")";
    }

    @Override
    public boolean equals( Object o ){
        if ( this == o ) return true;
        if ( o == null || getClass() != o.getClass() ) return false;

        WriteConcern that = (WriteConcern) o;
        return _fsync == that._fsync && _w == that._w && _wtimeout == that._wtimeout && _j == that._j && _continueOnErrorForInsert == that._continueOnErrorForInsert;
    }

    /**
     * Gets the j parameter (journal syncing)
     * @return
     */
    public boolean getJ() {
        return _j;
    }

    /**
     * Toggles the "continue inserts on error" mode. This only applies to server side errors.
     * If there is a document which does not validate in the client, an exception will still
     * be thrown in the client.
     * This will return a *NEW INSTANCE* of WriteConcern with your preferred continueOnInsert value
     *
     * @param continueOnErrorForInsert
     */
    public WriteConcern continueOnErrorForInsert(boolean continueOnErrorForInsert) {
        if ( _w instanceof Integer )
            return new WriteConcern((Integer) _w, _wtimeout, _fsync, _j, continueOnErrorForInsert);
        else if ( _w instanceof String )
            return new WriteConcern((String) _w, _wtimeout, _fsync, _j, continueOnErrorForInsert);
        else
            throw new IllegalStateException("The w parameter must be an int or a String");
    }

    /**
     * Gets the "continue inserts on error" mode
     * @return
     */
    public boolean getContinueOnErrorForInsert() {
        return _continueOnErrorForInsert;
    }

    /**
     * Create a Majority Write Concern that requires a majority of
     * servers to acknowledge the write.
     *
     * @param wtimeout timeout for write operation
     * @param fsync whether or not to fsync
     * @param j whether writes should wait for a journaling group commit
     */
    public static Majority majorityWriteConcern( int wtimeout, boolean fsync, boolean j ) {
        return new Majority( wtimeout, fsync, j );
    }


    Object _w = 0;
    int _wtimeout = 0;
    boolean _fsync = false;
    boolean _j = false;
    boolean _continueOnErrorForInsert = false;

    public static class Majority extends WriteConcern {

        public Majority( ) {
            super( "majority", 0, false, false );
        }

        public Majority( int wtimeout, boolean fsync, boolean j ){
            super( "majority", wtimeout, fsync, j );
        }

        @Override
        public String toString(){
            return "[Majority] WriteConcern " + getCommand();
        }

    }
}
