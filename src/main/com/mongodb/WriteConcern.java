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
     * Tag based Write Concern with configgable j and wtimeout=0, fsync=false
     * @param w Write Concern Tag
     * @param j whether writes should wait for a journaling group commit
     */
    public WriteConcern( String w, boolean j ){
        this( w , 0 , false, j );
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
    public WriteConcern( int w , int wtimeout , boolean fsync, boolean j ){
        _wValue = w;
        _wtimeout = wtimeout;
        _fsync = fsync;
        _j = j;
        _continueOnErrorInsert = false;
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
    public WriteConcern( int w , int wtimeout , boolean fsync, boolean j , boolean continueInsertOnError ){
        _wValue = w;
        _wtimeout = wtimeout;
        _fsync = fsync;
        _j = j;
        _continueOnErrorInsert = continueInsertOnError;
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
        _wValue = w;
        _wtimeout = wtimeout;
        _fsync = fsync;
        _j = j;
        _continueOnErrorInsert = false;
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
    public WriteConcern( String w , int wtimeout , boolean fsync, boolean j , Boolean continueInsertOnError ){
        _wValue = w;
        _wtimeout = wtimeout;
        _fsync = fsync;
        _j = j;
        _continueOnErrorInsert = continueInsertOnError;
    }


    /**
     * Gets the object representing the "getlasterror" command
     * @return
     */
    public BasicDBObject getCommand(){
        BasicDBObject _command = new BasicDBObject( "getlasterror" , 1 );

        if ( _wValue instanceof Integer && ( (Integer) _wValue > 0) ){
            _command.put( "w" , _wValue );
            _command.put( "wtimeout" , _wtimeout );
        }

        if ( _fsync )
            _command.put( "fsync" , true );

        if ( _j )
            _command.put( "j", true );

        return _command;
    }

    /**
     * Gets the number of servers to write to
     * if W is not a string value, returns -999.
     *
     * You should migrate to using getWValue (returns Object)
     * or getWString (String)
     * @return
     */
    public int getW(){
        if (_wValue instanceof Integer)
            return (Integer) _wValue;
        else
            return -999;
    }

    public String getWString(){
        return _wValue.toString();
    }


    /**
     * Gets the write timeout (in milliseconds)
     * @return
     */
    public int getWtimeout(){
        return _wtimeout;
    }

    /**
     * Returns whether writes wait for files to be synced to disk
     * @return
     */
    public boolean fsync(){
        return _fsync;
    }

    /**
     * Returns whether writes will await a group commit to the
     * journal.
     * @return boolean
     */
    public boolean j(){
        return _j;
    }
    
    /**
     * Returns whether (batch) inserts will continue if an error occurs before the end
     * @return boolean
     */
    public boolean continueOnErrorForInsert(){
        return _continueOnErrorInsert;
    }

    /**
     * Returns whether network error may be raised (w >= 0)
     * @return
     */
    public boolean raiseNetworkErrors(){
        return (Integer) _wValue >= 0;
    }

    /**
     * Returns whether "getlasterror" should be called (w > 0)
     * @return
     */
    public boolean callGetLastError(){
        return (Integer) _wValue  > 0;
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
        return "WriteConcern " + getCommand() + " / (Continue Inserting on Errors? " + continueOnErrorForInsert() + ")";
    }

    @Override
    public boolean equals( Object o ){
        if ( this == o ) return true;
        if ( o == null || getClass() != o.getClass() ) return false;

        WriteConcern that = (WriteConcern) o;
        return _fsync == that._fsync && _wValue == that._wValue && _wtimeout == that._wtimeout && _j == that._j && _continueOnErrorInsert == that._continueOnErrorInsert;
    }

    /**
     * Gets the w value (either int or string)
     * @return
     */
    public Object getWValue(){
        return _wValue;
    }

    /**
     * Clones this WriteConcern with a new String mode for "w" (WriteConcerns are immutable)
     * @param mode
     * @return
     */
    public WriteConcern withW(String mode) {
        return _instance(mode, _wtimeout, _fsync, _j, _continueOnErrorInsert);
    }

    /**
     * Clones this WriteConcern with a new int mode for "w" (WriteConcerns are immutable)
     * @param mode
     * @param w
     * @return
     */
    public WriteConcern withW(int w) {
        return _instance( w, _wtimeout, _fsync, _j, _continueOnErrorInsert );
    }

    /**
     *
     * Clones this WriteConcern with a new "j" (WriteConcerns are immutable)
     * @param j
     * @return
     */
    public WriteConcern withJ(boolean j) {
        return _instance( _wValue, _wtimeout, _fsync, _j, _continueOnErrorInsert );
    }


    /**
     *
     * Clones this WriteConcern with a new boolean mode for "continue inserts on error" (WriteConcerns are immutable)
     * @param cont
     * @return
     */
    public WriteConcern withContinueOnErrorForInsert(boolean cont) {
        return _instance( _wValue, _wtimeout, _fsync, _j, cont );
    }

    /**
     *
     * Clones this WriteConcern with a new boolean mode for fsync  (WriteConcerns are immutable)
     * @param fsync
     * @return
     */
    public WriteConcern withFsync(boolean fsync) {
        return _instance( _wValue, _wtimeout, fsync, _j, _continueOnErrorInsert );
    }


    protected WriteConcern _instance( Object wValue, int wtimeout, boolean fsync, boolean j, boolean cont ){
        if (wValue instanceof Integer)
            return new WriteConcern( (Integer) wValue, wtimeout, fsync, j, cont );
        else if (wValue instanceof String)
            return new WriteConcern( (String) wValue, wtimeout, fsync, j, cont );
        else throw new IllegalArgumentException( "W must be a String or Integer." );
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


    Object _wValue;
    final int _wtimeout;
    final boolean _fsync;
    final boolean _j;
    final boolean _continueOnErrorInsert;

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
