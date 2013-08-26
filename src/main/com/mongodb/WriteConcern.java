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

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

/**
 * <p>WriteConcern control the acknowledgment of write operations with various options.
 * <p>
 * <b>w</b>
 * <ul>
 *  <li>-1 = Don't even report network errors </li>
 *  <li> 0 = Don't wait for acknowledgement from the server </li>
 *  <li> 1 = Wait for acknowledgement, but don't wait for secondaries to replicate</li>
 *  <li> 2+= Wait for one or more secondaries to also acknowledge </li>
 * </ul>
 * <b>wtimeout</b> how long to wait for slaves before failing
 * <ul>
 *   <li>0: indefinite </li>
 *   <li>greater than 0: ms to wait </li>
 * </ul>
 * </p>
 * <p>
 * Other options:
 * <ul>
 *   <li><b>j</b>: wait for group commit to journal</li>
 *   <li><b>fsync</b>: force fsync to disk</li>
 * </ul>
 * @dochub databases
 */
public class WriteConcern implements Serializable {

    private static final long serialVersionUID = 1884671104750417011L;

    /**
     * No exceptions are raised, even for network issues.
     */
    public final static WriteConcern ERRORS_IGNORED = new WriteConcern(-1);

    /**
     * Write operations that use this write concern will wait for acknowledgement from the primary server before returning.
     * Exceptions are raised for network issues, and server errors.
     * @since 2.10.0
     */
    public final static WriteConcern ACKNOWLEDGED = new WriteConcern(1);
    /**
     * Write operations that use this write concern will return as soon as the message is written to the socket.
     * Exceptions are raised for network issues, but not server errors.
     * @since 2.10.0
     */
    public final static WriteConcern UNACKNOWLEDGED = new WriteConcern(0);

    /**
     * Exceptions are raised for network issues, and server errors; the write operation waits for the server to flush
     * the data to disk.
     */
    public final static WriteConcern FSYNCED = new WriteConcern(true);

    /**
     * Exceptions are raised for network issues, and server errors; the write operation waits for the server to
     * group commit to the journal file on disk.
     */
    public final static WriteConcern JOURNALED = new WriteConcern( 1, 0, false, true );

    /**
     * Exceptions are raised for network issues, and server errors; waits for at least 2 servers for the write operation.
     */
    public final static WriteConcern REPLICA_ACKNOWLEDGED= new WriteConcern(2);

    /**
     * No exceptions are raised, even for network issues.
     * <p>
     * This field has been superseded by {@code WriteConcern.ERRORS_IGNORED}, and may be deprecated in a future release.
     * @see WriteConcern#ERRORS_IGNORED
     */
    public final static WriteConcern NONE = new WriteConcern(-1);

    /**
     * Write operations that use this write concern will return as soon as the message is written to the socket.
     * Exceptions are raised for network issues, but not server errors.
     * <p>
     * This field has been superseded by {@code WriteConcern.UNACKNOWLEDGED}, and may be deprecated in a future release.
     * @see WriteConcern#UNACKNOWLEDGED
     */
    public final static WriteConcern NORMAL = new WriteConcern(0);

    /**
     * Write operations that use this write concern will wait for acknowledgement from the primary server before returning.
     * Exceptions are raised for network issues, and server errors.
     * <p>
     * This field has been superseded by {@code WriteConcern.ACKNOWLEDGED}, and may be deprecated in a future release.
     * @see WriteConcern#ACKNOWLEDGED
     */
    public final static WriteConcern SAFE = new WriteConcern(1);

    /**
     * Exceptions are raised for network issues, and server errors; waits on a majority of servers for the write operation.
     */
    public final static WriteConcern MAJORITY = new Majority();

    /**
     * Exceptions are raised for network issues, and server errors; the write operation waits for the server to flush
     * the data to disk.
     * <p>
     * This field has been superseded by {@code WriteConcern.FSYNCED}, and may be deprecated in a future release.
     * @see WriteConcern#FSYNCED
     */
    public final static WriteConcern FSYNC_SAFE = new WriteConcern(true);

    /**
     * Exceptions are raised for network issues, and server errors; the write operation waits for the server to
     * group commit to the journal file on disk.
     * <p>
     * This field has been superseded by {@code WriteConcern.JOURNALED}, and may be deprecated in a future release.
     * @see WriteConcern#JOURNALED
     */
    public final static WriteConcern JOURNAL_SAFE = new WriteConcern( 1, 0, false, true );

    /**
     * Exceptions are raised for network issues, and server errors; waits for at least 2 servers for the write operation.
     * <p>
     * This field has been superseded by {@code WriteConcern.REPLICA_ACKNOWLEDGED}, and may be deprecated in a future release.
     * @see WriteConcern#REPLICA_ACKNOWLEDGED
     */
    public final static WriteConcern REPLICAS_SAFE = new WriteConcern(2);

    // map of the constants from above for use by fromString
    private static Map<String, WriteConcern> _namedConcerns = null;

    /**
     * Default constructor keeping all options as default.  Be careful using this constructor, as it's equivalent to
     * {@code WriteConcern.UNACKNOWLEDGED}, so writes may be lost without any errors being reported.
     * @see WriteConcern#UNACKNOWLEDGED
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
     * @param continueOnError if batch writes should continue after the first error
     */
    public WriteConcern( int w , int wtimeout , boolean fsync , boolean j, boolean continueOnError) {
        _w = w;
        _wtimeout = wtimeout;
        _fsync = fsync;
        _j = j;
        _continueOnError = continueOnError;
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
     * @param continueOnError if batched writes should continue after the first error
     * @return
     */
    public WriteConcern( String w , int wtimeout , boolean fsync, boolean j, boolean continueOnError ){
        if (w == null) {
            throw new IllegalArgumentException("w can not be null");
        }

        _w = w;
        _wtimeout = wtimeout;
        _fsync = fsync;
        _j = j;
        _continueOnError = continueOnError;
    }

    /**
     * Gets the getlasterror command for this write concern.
     *
     * @return getlasterror command, even if <code>w <= 0</code>
     */
    public BasicDBObject getCommand() {
        BasicDBObject _command = new BasicDBObject( "getlasterror" , 1 );

        if (_w instanceof Integer && ((Integer) _w > 1) || (_w instanceof String)){
            _command.put( "w" , _w );
        }

        if (_wtimeout > 0) {
            _command.put( "wtimeout" , _wtimeout );
        }

        if ( _fsync )
            _command.put( "fsync" , true );

        if ( _j )
            _command.put( "j", true );

        return _command;
    }

    /**
     * Sets the w value (the write strategy).
     *
     * @param w  the value of w.
     * @deprecated construct a new instance instead.  This method will be removed in a future major release, as instances of this class
     * should really be immutable.
     */
    @Deprecated
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
     * @return w as a string
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
                        String key = f.getName().toLowerCase();
                        newMap.put(key, (WriteConcern) f.get( null ) );
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
    public String toString() {
        return "WriteConcern " + getCommand() + " / (Continue on error? " + getContinueOnErrorForInsert() + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WriteConcern that = (WriteConcern) o;

        if (_continueOnError != that._continueOnError) return false;
        if (_fsync != that._fsync) return false;
        if (_j != that._j) return false;
        if (_wtimeout != that._wtimeout) return false;
        if (!_w.equals(that._w)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = _w.hashCode();
        result = 31 * result + _wtimeout;
        result = 31 * result + (_fsync ? 1 : 0);
        result = 31 * result + (_j ? 1 : 0);
        result = 31 * result + (_continueOnError ? 1 : 0);
        return result;
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
     * This will return a new instance of WriteConcern with your preferred continueOnInsert value
     *
     * @param continueOnError
     */
    public WriteConcern continueOnError(boolean continueOnError) {
        if ( _w instanceof Integer )
            return new WriteConcern((Integer) _w, _wtimeout, _fsync, _j, continueOnError);
        else if ( _w instanceof String )
            return new WriteConcern((String) _w, _wtimeout, _fsync, _j, continueOnError);
        else
            throw new IllegalStateException("The w parameter must be an int or a String");
    }

    /**
     * Gets the "continue inserts on error" mode
     *
     * @return the continue on error mode
     */
    public boolean getContinueOnError() {
        return _continueOnError;
    }

    /**
     * Toggles the "continue inserts on error" mode. This only applies to server side errors.
     * If there is a document which does not validate in the client, an exception will still
     * be thrown in the client.
     * This will return a new instance of WriteConcern with your preferred continueOnInsert value
     *
     * @param continueOnErrorForInsert
     * @deprecated Use continueOnError instead
     */
    @Deprecated
    public WriteConcern continueOnErrorForInsert(boolean continueOnErrorForInsert) {
        return continueOnError(continueOnErrorForInsert);
    }

    /**
     * Gets the "continue inserts on error" mode
     * @return
     * @deprecated Use getContinueOnError instead
     */
    @Deprecated
    public boolean getContinueOnErrorForInsert() {
        return getContinueOnError();
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


    Object _w;  // this should be final, but can't be because of inadvertent public setter
    final int _wtimeout;
    final boolean _fsync;
    final boolean _j;
    final boolean _continueOnError;

    public static class Majority extends WriteConcern {

        private static final long serialVersionUID = -4128295115883875212L;

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
