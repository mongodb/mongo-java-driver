// DBApiLayer.java

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

import java.nio.*;
import java.util.*;

import com.mongodb.util.*;

/** Database API
 * This cannot be directly instantiated, but the functions are available
 * through instances of Mongo.
 */
public abstract class DBApiLayer extends DBBase {

    static final boolean D = Boolean.getBoolean( "DEBUG.DB" );
    /** The maximum number of cursors allowed */
    static final int NUM_CURSORS_BEFORE_KILL = 100;

    static final boolean SHOW = Boolean.getBoolean( "DB.SHOW" );

    protected DBApiLayer( String root ){
        super( root );

        _root = root;
    }

    protected abstract void doInsert( ByteBuffer buf ) throws MongoException;
    protected abstract void doDelete( ByteBuffer buf ) throws MongoException;
    protected abstract void doUpdate( ByteBuffer buf ) throws MongoException;
    protected abstract void doKillCursors( ByteBuffer buf ) throws MongoException;
    protected abstract int doQuery( ByteBuffer out , ByteBuffer in ) throws MongoException;
    protected abstract int doGetMore( ByteBuffer out , ByteBuffer in ) throws MongoException;

    public abstract String debugString();

    protected MyCollection doGetCollection( String name ){
        MyCollection c = _collections.get( name );
        if ( c != null )
            return c;

        synchronized ( _collections ){
            c = _collections.get( name );
            if ( c != null )
                return c;

            c = new MyCollection( name );
            _collections.put( name , c );
        }

        return c;
    }

    String _removeRoot( String ns ){
        if ( ! ns.startsWith( _root + "." ) )
            return ns;
        return ns.substring( _root.length() + 1 );
    }

    /**
     *  Authenticates connection/db with given name and password
     *
     * @param username  name of user for this database
     * @param passwd password of user for this database
     * @return true if authenticated, false otherwise
     */
    public boolean authenticate(String username, String passwd)
        throws MongoException {

        BasicDBObject res = (BasicDBObject) command(new BasicDBObject("getnonce", 1));

        if (res.getInt("ok") != 1) {
            throw new MongoException("Error - unable to get nonce value for authentication.");
        }

        String nonce = res.getString("nonce");

        String auth = username + ":mongo:" + passwd;
        String key = nonce + username + Util.hexMD5(auth.getBytes());

        BasicDBObject cmd = new BasicDBObject();

        cmd.put("authenticate", 1);
        cmd.put("user", username);
        cmd.put("nonce", nonce);
        cmd.put("key", Util.hexMD5(key.getBytes()));

        res = (BasicDBObject) command(cmd);

        return res.getInt("ok") == 1;
    }

    /**
     *  Gets the the error (if there is one) from the previous operation.  The result of
     *  this command will look like
     *
     *  <pre>
     * { "err" :  errorMessage  , "ok" : 1.0 , "_ns" : "$cmd"}
     * </pre>
     *
     * The value for errorMessage will be null if no error occurred, or a description otherwise.
     *
     * Care must be taken to ensure that calls to getLastError go to the same connection as that
     * of the previous operation. See com.mongodb.Mongo.requestStart for more information.
     *
     *  @return DBObject with error and status information
     */
    public DBObject getLastError()
        throws MongoException {
        return command(new BasicDBObject("getlasterror", 1));
    }

    /**
     *  Returns the last error that occurred since start of database or a call to <code>resetError()</code>
     *
     *  The return object will look like
     *
     *  <pre>
     * { err : errorMessage, nPrev : countOpsBack, ok : 1 }
     *  </pre>
     *
     * The value for errormMessage will be null of no error has ocurred, or the message.  The value of
     * countOpsBack will be the number of operations since the error occurred.
     *
     * Care must be taken to ensure that calls to getPreviousError go to the same connection as that
     * of the previous operation. See com.mongodb.Mongo.requestStart for more information.
     *
     * @return DBObject with error and status information
     */
    public DBObject getPreviousError()
        throws MongoException {
        return command(new BasicDBObject("getpreverror", 1));
    }

    /**
     *  Resets the error memory for this database.  Used to clear all errors such that getPreviousError()
     *  will return no error.
     */
    public void resetError()
        throws MongoException {
        command(new BasicDBObject("reseterror", 1));
    }

    /**
     *  For testing purposes only - this method forces an error to help test error handling
     */
    public void forceError()
        throws MongoException {
        command(new BasicDBObject("forceerror", 1));
    }

    /**
     *  Drops this database.  Removes all data on disk.  Use with caution.
     */
    public void dropDatabase()
        throws MongoException {

        BasicDBObject res = (BasicDBObject) command(new BasicDBObject("dropDatabase", 1));

        if (res.getInt("ok") != 1) {
            throw new RuntimeException("Error - unable to drop database : " + res.toString());
        }
    }

    /** Get a collection from a &lt;databaseName&gt;.&lt;collectionName&gt;.
     * If <code>fullNameSpace</code> does not contain any "."s, this will
     * find a collection called <code>fullNameSpace</code> and return it.
     * Otherwise, it will find the collecton <code>collectionName</code> and
     * return it.
     * @param fullNameSpace the full name to find
     * @throws RuntimeException if the database named is not this database
     */
    public MyCollection getCollectionFromFull( String fullNameSpace ){
        // TOOD security

        if ( fullNameSpace.indexOf( "." ) < 0 ) {
            // assuming local
            return doGetCollection( fullNameSpace );
        }

        final int idx = fullNameSpace.indexOf( "." );

        final String root = fullNameSpace.substring( 0 , idx );
        final String table = fullNameSpace.substring( idx + 1 );

        if (_root.equals(root)) {
            return doGetCollection( table );
        }

        throw new RuntimeException( "can't get sister dbs yet" );
    }

    /** Returns a set of the names of collections in this database.
     * @return the names of collections in this database
     */
    public Set<String> getCollectionNames()
        throws MongoException {

        DBCollection namespaces = getCollection("system.namespaces");
        if (namespaces == null)
            throw new RuntimeException("this is impossible");

        Iterator<DBObject> i = namespaces.find(new BasicDBObject(), null, 0, 0);
        if (i == null)
            return new HashSet<String>();

        List<String> tables = new ArrayList<String>();

        for (; i.hasNext();) {
            DBObject o = i.next();
            String n = o.get("name").toString();
            int idx = n.indexOf(".");

            String root = n.substring(0, idx);
            if (!root.equals(_root))
                continue;

            if (n.indexOf("$") >= 0)
                continue;

            String table = n.substring(idx + 1);

            tables.add(table);
        }

        Collections.sort(tables);

        return new OrderedSet<String>(tables);
    }

    class MyCollection extends DBCollection {
        MyCollection( String name ){
            super( DBApiLayer.this , name );
            _fullNameSpace = _root + "." + name;
        }

        public void doapply( DBObject o ){
            o.put( "_ns" , _removeRoot( _fullNameSpace ) );
        }

        public DBObject insert( DBObject o )
            throws MongoException {
            DBObject[] arr = new DBObject[1];
            arr[0] = o;
            return insert(arr)[0];
        }

        public DBObject[] insert(DBObject[] arr)
            throws MongoException {
            return insert(arr, true);
        }

        public List<DBObject> insert(List<DBObject> list)
            throws MongoException {
            DBObject[] arr = insert(list.toArray(new DBObject[list.size()]) , true);

            return Arrays.asList(arr);
        }

        protected DBObject insert(DBObject obj, boolean shouldApply )
            throws MongoException {
            DBObject[] arr = new DBObject[1];
            arr[0] = obj;
            return insert(arr, shouldApply)[0];
        }

        protected DBObject[] insert(DBObject[] arr, boolean shouldApply )
            throws MongoException {

            if ( SHOW ) {
                for (DBObject o : arr) {
                    System.out.println( "save:  " + _fullNameSpace + " " + JSON.serialize( o ) );
                }
            }

            if ( shouldApply ){
                for (DBObject o : arr) {
                    apply( o );
                    Object id = o.get( "_id" );
                    if ( id instanceof ObjectId )
                        ((ObjectId)id)._new = false;
                }
            }

            ByteEncoder encoder = ByteEncoder.get();

            encoder._buf.putInt( 0 ); // reserved
            encoder._put( _fullNameSpace );

            for (DBObject o : arr) {
                encoder.putObject( o );
            }
            encoder.flip();

            try {
                doInsert( encoder._buf );
            }
            finally {
                encoder.done();
            }

            return arr;
        }

        public void remove( DBObject o )
            throws MongoException {

            if ( SHOW ) System.out.println( "remove: " + _fullNameSpace + " " + JSON.serialize( o ) );

            ByteEncoder encoder = ByteEncoder.get();
            encoder._buf.putInt( 0 ); // reserved
            encoder._put( _fullNameSpace );

            Collection<String> keys = o.keySet();

            if ( keys.size() == 1 &&
                 keys.iterator().next().equals( "_id" ) &&
                 o.get( keys.iterator().next() ) instanceof ObjectId )
                encoder._buf.putInt( 1 );
            else
                encoder._buf.putInt( 0 );

            encoder.putObject( o );
            encoder.flip();

            try {
                doDelete( encoder._buf );
            }
            finally {
                encoder.done();
            }
        }

        void _cleanCursors()
            throws MongoException {
            if ( _deadCursorIds.size() == 0 )
                return;

            if ( _deadCursorIds.size() % 20 != 0 && _deadCursorIds.size() < NUM_CURSORS_BEFORE_KILL )
                return;

            List<Long> l = _deadCursorIds;
            _deadCursorIds = new Vector<Long>();

            System.out.println( "trying to kill cursors : " + l.size() );

            try {
                killCursors( l );
            }
            catch ( Throwable t ){
                t.printStackTrace();
                _deadCursorIds.addAll( l );
            }
        }

        void killCursors( List<Long> all )
            throws MongoException {
            if ( all == null || all.size() == 0 )
                return;

            ByteEncoder encoder = ByteEncoder.get();
            encoder._buf.putInt( 0 ); // reserved

            encoder._buf.putInt( all.size() );

            for (Long l : all) {
                encoder._buf.putLong(l);
            }

            try {
                doKillCursors( encoder._buf );
            }
            finally {
                encoder.done();
            }
        }

        Iterator<DBObject> find( DBObject ref , DBObject fields , int numToSkip , int numToReturn )
            throws MongoException {

            if ( SHOW ) System.out.println( "find: " + _fullNameSpace + " " + JSON.serialize( ref ) );

            _cleanCursors();

            ByteEncoder encoder = ByteEncoder.get();

            encoder._buf.putInt( 0 ); // options
            encoder._put( _fullNameSpace );

            encoder._buf.putInt( numToSkip );
            encoder._buf.putInt( numToReturn );
            encoder.putObject( ref ); // ref
            if ( fields != null )
                encoder.putObject( fields ); // fields to return
            encoder.flip();

            ByteDecoder decoder = ByteDecoder.get( DBApiLayer.this , this );

            try {
                int len = doQuery( encoder._buf , decoder._buf );
                decoder.doneReading( len );

                SingleResult res = new SingleResult( _fullNameSpace , decoder);

                if ( res._lst.size() == 0 )
                    return null;

                if ( res._lst.size() == 1 ){
                    Object err = res._lst.get(0).get( "$err" );
                    if ( err != null )
                        throw new RuntimeException( "db error [" + err + "]" );
                }

                return new Result( this , res , numToReturn );
            }
            finally {
                decoder.done();
                encoder.done();
            }
        }

        public DBObject update( DBObject query , DBObject o , boolean upsert , boolean apply )
            throws MongoException {

            if ( SHOW ) System.out.println( "update: " + _fullNameSpace + " " + JSON.serialize( query ) );

            if ( apply ){
                apply( o );
                ObjectId id = ((ObjectId)o.get( "_id" ));
                id._new = false;
            }

            ByteEncoder encoder = ByteEncoder.get();
            encoder._buf.putInt( 0 ); // reserved
            encoder._put( _fullNameSpace );

            encoder._buf.putInt( upsert ? 1 : 0 );

            encoder.putObject( query );
            encoder.putObject( o );

            encoder.flip();

            try {
                doUpdate( encoder._buf );
            }
            finally {
                encoder.done();
            }

            return o;
        }

        public void ensureIndex( DBObject keys , String name )
            throws MongoException {
            ensureIndex(keys, name, false);
        }

        public void ensureIndex( DBObject keys, String name, boolean unique)
            throws MongoException {

            DBObject o = new BasicDBObject();
            o.put( "name" , name );
            o.put( "ns" , _fullNameSpace );
            o.put( "key" , keys );
            o.put( "unique", unique);

        //dm-system isnow in our database
        DBApiLayer.this.doGetCollection( "system.indexes" ).insert( o , false );
        }

        final String _fullNameSpace;
    }

    class QueryHeader {

        QueryHeader( ByteBuffer buf ){
            this( buf , buf.position() );
        }

        QueryHeader( ByteBuffer buf , int start ){
            _reserved = buf.getInt( start );
            _cursor = buf.getLong( start + 4 );
            _startingFrom = buf.getInt( start + 12 );
            _num = buf.getInt( 16 );
        }

        int headerSize(){
            return 20;
        }

        void skipPastHeader( ByteBuffer buf ){
            buf.position( buf.position() + headerSize() );
        }

        final int _reserved;
        final long _cursor;
        final int _startingFrom;
        final int _num;
    }

    class SingleResult extends QueryHeader {

        SingleResult( String fullNameSpace , ByteDecoder decoder){
            super( decoder._buf );

            _bytes = decoder.remaining();
            _fullNameSpace = fullNameSpace;
            skipPastHeader( decoder._buf );

            if ( _num == 0 )
                _lst = EMPTY;
            else if ( _num < 3 )
                _lst = new LinkedList<DBObject>();
            else
                _lst = new ArrayList<DBObject>( _num );

            if ( _num > 0 ){
                int num = 0;

                while( decoder.more() && num < _num ){
                    final DBObject o = decoder.readObject();

                    o.put( "_ns" , _removeRoot( _fullNameSpace ) );
                    _lst.add( o );
                    num++;

                    if ( D ) {
                        System.out.println( "-- : " + o.keySet().size() );
                        for ( String s : o.keySet() )
                            System.out.println( "\t " + s + " : " + o.get( s ) );
                    }
                }
            }
        }

        boolean hasGetMore(){
            return _num > 0 && _cursor > 0;
        }

        public String toString(){
            return "reserved:" + _reserved + " _cursor:" + _cursor + " _startingFrom:" + _startingFrom + " _num:" + _num ;
        }

        final long _bytes;
        final String _fullNameSpace;

        final List<DBObject> _lst;
    }

    class Result implements Iterator<DBObject> {

        Result( MyCollection coll , SingleResult res , int numToReturn ){
            init( res );
            _collection = coll;
            _numToReturn = numToReturn;
        }

        private void init( SingleResult res ){
            _totalBytes += res._bytes;
            _curResult = res;
            _cur = res._lst.iterator();
        }

        public DBObject next(){
            if ( _cur.hasNext() )
                return _cur.next();

            if ( ! _curResult.hasGetMore() )
                throw new RuntimeException( "no more" );

            _advance();
            return next();
        }

        public boolean hasNext(){
            if ( _cur.hasNext() )
                return true;

            if ( ! _curResult.hasGetMore() )
                return false;

            _advance();
            return hasNext();
        }

        private void _advance(){

            if ( _curResult._cursor <= 0 )
                throw new RuntimeException( "can't advance a cursor <= 0" );

            ByteEncoder encoder = ByteEncoder.get();

            encoder._buf.putInt( 0 ); // reserved
            encoder._put( _curResult._fullNameSpace );
            encoder._buf.putInt( _numToReturn ); // num to return
            encoder._buf.putLong( _curResult._cursor );
            encoder.flip();

            ByteDecoder decoder = ByteDecoder.get( DBApiLayer.this , _collection );

            try {
                int len = doGetMore( encoder._buf , decoder._buf );
                decoder.doneReading( len );

                SingleResult res = new SingleResult( _curResult._fullNameSpace , decoder);
                init( res );
            }
            catch ( MongoException me ){
                throw new MongoInternalException( "can't do getmore" , me );
            }
            finally {
                decoder.done();
                encoder.done();
            }
        }

        public void remove(){
            throw new RuntimeException( "can't remove this way" );
        }

        public String toString(){
            return "DBCursor";
        }

        protected void finalize() throws Throwable {
            if ( _curResult != null && _curResult._cursor > 0 )
                _deadCursorIds.add( _curResult._cursor );
            super.finalize();
        }

        public long totalBytes(){
            return _totalBytes;
        }

        SingleResult _curResult;
        Iterator<DBObject> _cur;
        final MyCollection _collection;
        final int _numToReturn;

        private long _totalBytes = 0;
    }  // class Result

    final String _root;
    final Map<String,MyCollection> _collections = Collections.synchronizedMap( new HashMap<String,MyCollection>() );
    final Map<String,DBApiLayer> _sisters = Collections.synchronizedMap( new HashMap<String,DBApiLayer>() );
    List<Long> _deadCursorIds = new Vector<Long>();

    static final List<DBObject> EMPTY = Collections.unmodifiableList( new LinkedList<DBObject>() );
}
