// DBCursor.java

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

import com.mongodb.DBApiLayer.Result;
import org.bson.util.annotations.NotThreadSafe;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;


/** An iterator over database results.
 * Doing a <code>find()</code> query on a collection returns a
 * <code>DBCursor</code> thus
 *
 * <blockquote><pre>
 * DBCursor cursor = collection.find( query );
 * if( cursor.hasNext() )
 *     DBObject obj = cursor.next();
 * </pre></blockquote>
 *
 * <p><b>Warning:</b> Calling <code>toArray</code> or <code>length</code> on
 * a DBCursor will irrevocably turn it into an array.  This
 * means that, if the cursor was iterating over ten million results
 * (which it was lazily fetching from the database), suddenly there will
 * be a ten-million element array in memory.  Before converting to an array,
 * make sure that there are a reasonable number of results using
 * <code>skip()</code> and <code>limit()</code>.
 * <p>For example, to get an array of the 1000-1100th elements of a cursor, use
 *
 * <blockquote><pre>
 * List<DBObject> obj = collection.find( query ).skip( 1000 ).limit( 100 ).toArray();
 * </pre></blockquote>
 *
 *
 * @dochub cursors
 */
@NotThreadSafe
public class DBCursor implements Iterator<DBObject> , Iterable<DBObject>, Closeable {

    /**
     * Initializes a new database cursor
     * @param collection collection to use
     * @param q query to perform
     * @param k keys to return from the query
     * @param preference the Read Preference for this query
     */
    public DBCursor( DBCollection collection , DBObject q , DBObject k, ReadPreference preference ){
        if (collection == null) {
            throw new IllegalArgumentException("collection is null");
        }
        _collection = collection;
        _query = q == null ? new BasicDBObject() : q;
        _keysWanted = k;
        _options = _collection.getOptions();
        _readPref = preference;
        _decoderFact = collection.getDBDecoderFactory();
    }

    /**
     * Types of cursors: iterator or array.
     */
    static enum CursorType { ITERATOR , ARRAY };

    /**
     * Creates a copy of an existing database cursor.
     * The new cursor is an iterator, even if the original
     * was an array.
     *
     * @return the new cursor
     */
    public DBCursor copy() {
        DBCursor c = new DBCursor(_collection, _query, _keysWanted, _readPref);
        c._orderBy = _orderBy;
        c._hint = _hint;
        c._hintDBObj = _hintDBObj;
        c._limit = _limit;
        c._skip = _skip;
        c._options = _options;
        c._batchSize = _batchSize;
        c._snapshot = _snapshot;
        c._explain = _explain;
        if ( _specialFields != null )
            c._specialFields = new BasicDBObject( _specialFields.toMap() );
        return c;
    }

    /**
     * creates a copy of this cursor object that can be iterated.
     * Note:
     * - you can iterate the DBCursor itself without calling this method
     * - no actual data is getting copied.
     *
     * @return
     */
    public Iterator<DBObject> iterator(){
        return this.copy();
    }

    // ---- querty modifiers --------

    /**
     * Sorts this cursor's elements.
     * This method must be called before getting any object from the cursor.
     * @param orderBy the fields by which to sort
     * @return a cursor pointing to the first element of the sorted results
     */
    public DBCursor sort( DBObject orderBy ){
        if ( _it != null )
            throw new IllegalStateException( "can't sort after executing query" );

        _orderBy = orderBy;
        return this;
    }

    /**
     * adds a special operator like $maxScan or $returnKey
     * e.g. addSpecial( "$returnKey" , 1 )
     * e.g. addSpecial( "$maxScan" , 100 )
     * @param name
     * @param o
     * @return
     * @dochub specialOperators
     */
    public DBCursor addSpecial( String name , Object o ){
        if ( _specialFields == null )
            _specialFields = new BasicDBObject();
        _specialFields.put( name , o );
        return this;
    }

    /**
     * Informs the database of indexed fields of the collection in order to improve performance.
     * @param indexKeys a <code>DBObject</code> with fields and direction
     * @return same DBCursor for chaining operations
     */
    public DBCursor hint( DBObject indexKeys ){
        if ( _it != null )
            throw new IllegalStateException( "can't hint after executing query" );
        
        _hintDBObj = indexKeys;
        return this;
    }

    /**
     *  Informs the database of an indexed field of the collection in order to improve performance.
     * @param indexName the name of an index
     * @return same DBCursor for chaining operations
     */
    public DBCursor hint( String indexName ){
        if ( _it != null )
            throw new IllegalStateException( "can't hint after executing query" );

        _hint = indexName;
        return this;
    }

    /**
     * Use snapshot mode for the query. Snapshot mode assures no duplicates are
     * returned, or objects missed, which were present at both the start and end
     * of the query's execution (if an object is new during the query, or deleted
     * during the query, it may or may not be returned, even with snapshot mode).
     * Note that short query responses (less than 1MB) are always effectively snapshotted.
     * Currently, snapshot mode may not be used with sorting or explicit hints.
     * @return same DBCursor for chaining operations
     */
    public DBCursor snapshot() {
        if (_it != null)
            throw new IllegalStateException("can't snapshot after executing the query");

        _snapshot = true;

        return this;
    }

    /**
     * Returns an object containing basic information about the
     * execution of the query that created this cursor
     * This creates a <code>DBObject</code> with the key/value pairs:
     * "cursor" : cursor type
     * "nScanned" : number of records examined by the database for this query
     * "n" : the number of records that the database returned
     * "millis" : how long it took the database to execute the query
     * @return a <code>DBObject</code>
     * @throws MongoException
     * @dochub explain
     */
    public DBObject explain(){
        DBCursor c = copy();
        c._explain = true;
        if (c._limit > 0) {
            // need to pass a negative batchSize as limit for explain
            c._batchSize = c._limit * -1;
            c._limit = 0;
        }
        return c.next();
    }

    /**
     * Limits the number of elements returned.
     * Note: parameter <tt>n</tt> should be positive, although a negative value is supported for legacy reason.
     * Passing a negative value will call {@link DBCursor#batchSize(int)} which is the preferred method.
     * @param n the number of elements to return
     * @return a cursor to iterate the results
     * @dochub limit
     */
    public DBCursor limit( int n ){
        if ( _it != null )
            throw new IllegalStateException( "can't set limit after executing query" );

        if (n > 0)
            _limit = n;
        else if (n < 0)
            batchSize(n);
        return this;
    }

    /**
     * Limits the number of elements returned in one batch.
     * A cursor typically fetches a batch of result objects and store them locally.
     *
     * If <tt>batchSize</tt> is positive, it represents the size of each batch of objects retrieved.
     * It can be adjusted to optimize performance and limit data transfer.
     *
     * If <tt>batchSize</tt> is negative, it will limit of number objects returned, that fit within the max batch size limit (usually 4MB), and cursor will be closed.
     * For example if <tt>batchSize</tt> is -10, then the server will return a maximum of 10 documents and as many as can fit in 4MB, then close the cursor.
     * Note that this feature is different from limit() in that documents must fit within a maximum size, and it removes the need to send a request to close the cursor server-side.
     *
     * The batch size can be changed even after a cursor is iterated, in which case the setting will apply on the next batch retrieval.
     *
     * @param n the number of elements to return in a batch
     * @return
     */
    public DBCursor batchSize( int n ){
        // check for special case, used to have server bug with 1
        if ( n == 1 )
            n = 2;

        if ( _it != null ) {
        	if (_it instanceof DBApiLayer.Result)
        		((DBApiLayer.Result)_it).setBatchSize(n);
        }

        _batchSize = n;
        return this;
    }

    /**
     * Discards a given number of elements at the beginning of the cursor.
     * @param n the number of elements to skip
     * @return a cursor pointing to the new first element of the results
     * @throws IllegalStateException if the cursor has started to be iterated through
     */
    public DBCursor skip( int n ){
        if ( _it != null )
            throw new IllegalStateException( "can't set skip after executing query" );
        _skip = n;
        return this;
    }

    /**
     * gets the cursor id.
     * @return the cursor id, or 0 if there is no active cursor.
     */
    public long getCursorId() {
    	if ( _it instanceof Result )
            return ((Result)_it).getCursorId();

    	return 0;
    }

    /**
     * kills the current cursor on the server.
     */
    public void close() {
    	if ( _it instanceof Result )
            ((Result)_it).close();
    }

    /**
     * makes this query ok to run on a slave node
     *
     * @return a copy of the same cursor (for chaining)
     *
     * @deprecated Replaced with {@code ReadPreference.secondaryPreferred()}
     * @see ReadPreference#secondaryPreferred()
     */
    @Deprecated
    public DBCursor slaveOk(){
        return addOption( Bytes.QUERYOPTION_SLAVEOK );
    }

    /**
     * adds a query option - see Bytes.QUERYOPTION_* for list
     * @param option
     * @return
     */
    public DBCursor addOption( int option ){
        if ( option == Bytes.QUERYOPTION_EXHAUST )
            throw new IllegalArgumentException("The exhaust option is not user settable.");
        
        _options |= option;
        return this;
    }

    /**
     * sets the query option - see Bytes.QUERYOPTION_* for list
     * @param options
     */
    public DBCursor setOptions( int options ){
        _options = options;
        return this;
    }

    /**
     * resets the query options
     */
    public DBCursor resetOptions(){
        _options = 0;
        return this;
    }

    /**
     * gets the query options
     * @return
     */
    public int getOptions(){
        return _options;
    }

    // ----  internal stuff ------

    private void _check() {
        if (_it != null)
            return;

        _lookForHints();

        QueryOpBuilder builder = new QueryOpBuilder()
                .addQuery(_query)
                .addOrderBy(_orderBy)
                .addHint(_hintDBObj)
                .addHint(_hint)
                .addExplain(_explain)
                .addSnapshot(_snapshot)
                .addSpecialFields(_specialFields);

        if (_collection.getDB().getMongo().isMongosConnection()) {
            builder.addReadPreference(_readPref);
        }

        _it = _collection.__find(builder.get(), _keysWanted, _skip, _batchSize, _limit,
                _options, _readPref, getDecoder());
    }

    // Only create a new decoder if there is a decoder factory explicitly set on the collection.  Otherwise return null
    // so that the collection can use a cached decoder
    private DBDecoder getDecoder() {
        return _decoderFact != null ? _decoderFact.create() : null;
    }

    /**
     * if there is a hint to use, use it
     */
    private void _lookForHints(){

        if ( _hint != null ) // if someone set a hint, then don't do this
            return;

        if ( _collection._hintFields == null )
            return;

        Set<String> mykeys = _query.keySet();

        for ( DBObject o : _collection._hintFields ){

            Set<String> hintKeys = o.keySet();

            if ( ! mykeys.containsAll( hintKeys ) )
                continue;

            hint( o );
            return;
        }
    }

    void _checkType( CursorType type ){
        if ( _cursorType == null ){
            _cursorType = type;
            return;
        }

        if ( type == _cursorType )
            return;

        throw new IllegalArgumentException( "can't switch cursor access methods" );
    }

    private DBObject _next() {
        if ( _cursorType == null )
            _checkType( CursorType.ITERATOR );

        _check();

        _cur = _it.next();
        _num++;

        if ( _keysWanted != null && _keysWanted.keySet().size() > 0 ){
            _cur.markAsPartialObject();
            //throw new UnsupportedOperationException( "need to figure out partial" );
        }

        if ( _cursorType == CursorType.ARRAY ){
            _all.add( _cur );
        }

        return _cur;
    }

    /**
     * gets the number of times, so far, that the cursor retrieved a batch from the database
     * @return
     */
    public int numGetMores(){
        if ( _it instanceof DBApiLayer.Result )
            return ((DBApiLayer.Result)_it).numGetMores();

        throw new IllegalArgumentException("_it not a real result" );
    }

    /**
     * gets a list containing the number of items received in each batch
     * @return
     */
    public List<Integer> getSizes(){
        if ( _it instanceof DBApiLayer.Result )
            return ((DBApiLayer.Result)_it).getSizes();

        throw new IllegalArgumentException("_it not a real result" );
    }

    private boolean _hasNext() {
        _check();

        if ( _limit > 0 && _num >= _limit )
            return false;

        return _it.hasNext();
    }

    /**
     * Returns the number of objects through which the cursor has iterated.
     * @return the number of objects seen
     */
    public int numSeen(){
        return _num;
    }

    // ----- iterator api -----

    /**
     * Checks if there is another object available
     * @return
     * @throws MongoException
     */
    public boolean hasNext() {
        _checkType( CursorType.ITERATOR );
        return _hasNext();
    }

    /**
     * Returns the object the cursor is at and moves the cursor ahead by one.
     * @return the next element
     * @throws MongoException
     */
    public DBObject next() {
        _checkType( CursorType.ITERATOR );
        return _next();
    }

    /**
     * Returns the element the cursor is at.
     * @return the current element
     */
    public DBObject curr(){
        _checkType( CursorType.ITERATOR );
        return _cur;
    }

    /**
     * Not implemented.
     */
    public void remove(){
        throw new UnsupportedOperationException( "can't remove from a cursor" );
    }


    //  ---- array api  -----

    void _fill( int n ){
        _checkType( CursorType.ARRAY );
        while ( n >= _all.size() && _hasNext() )
            _next();
    }

    /**
     * pulls back all items into an array and returns the number of objects.
     * Note: this can be resource intensive
     * @see #count()
     * @see #size()
     * @return the number of elements in the array
     * @throws MongoException
     */
    public int length() {
        _checkType( CursorType.ARRAY );
        _fill( Integer.MAX_VALUE );
        return _all.size();
    }

    /**
     * Converts this cursor to an array.
     * @return an array of elements
     * @throws MongoException
     */
    public List<DBObject> toArray(){
        return toArray( Integer.MAX_VALUE );
    }

    /**
     * Converts this cursor to an array.
     * @param max the maximum number of objects to return
     * @return an array of objects
     * @throws MongoException
     */
    public List<DBObject> toArray( int max ) {
        _checkType( CursorType.ARRAY );
        _fill( max - 1 );
        return _all;
    }

    /**
     * for testing only!
     * Iterates cursor and counts objects
     * @see #count()
     * @return num objects
     * @throws MongoException
     */
    public int itcount(){
        int n = 0;
        while ( this.hasNext() ){
            this.next();
            n++;
        }
        return n;
    }

    /**
     * Counts the number of objects matching the query
     * This does not take limit/skip into consideration
     * @see #size()
     * @return the number of objects
     * @throws MongoException
     */
    public int count() {
        if ( _collection == null )
            throw new IllegalArgumentException( "why is _collection null" );
        if ( _collection._db == null )
            throw new IllegalArgumentException( "why is _collection._db null" );

        return (int)_collection.getCount(this._query, this._keysWanted, getReadPreference());
    }

    /**
     * Counts the number of objects matching the query
     * this does take limit/skip into consideration
     * @see #count()
     * @return the number of objects
     * @throws MongoException
     */
    public int size() {
        if ( _collection == null )
            throw new IllegalArgumentException( "why is _collection null" );
        if ( _collection._db == null )
            throw new IllegalArgumentException( "why is _collection._db null" );

        return (int)_collection.getCount(this._query, this._keysWanted, this._limit, this._skip, getReadPreference() );
    }


    /**
     * gets the fields to be returned
     * @return
     */
    public DBObject getKeysWanted(){
        return _keysWanted;
    }

    /**
     * gets the query
     * @return
     */
    public DBObject getQuery(){
        return _query;
    }

    /**
     * gets the collection
     * @return
     */
    public DBCollection getCollection(){
        return _collection;
    }

    /**
     * Gets the Server Address of the server that data is pulled from.
     * Note that this information may not be available until hasNext() or next() is called.
     * @return
     */
    public ServerAddress getServerAddress() {
        if (_it != null && _it instanceof DBApiLayer.Result)
            return ((DBApiLayer.Result)_it).getServerAddress();

        return null;
    }

    /**
     * Sets the read preference for this cursor.
     * See the * documentation for {@link ReadPreference}
     * for more information.
     *
     * @param preference Read Preference to use
     */
    public DBCursor setReadPreference( ReadPreference preference ){
        _readPref = preference;
        return this;
    }

    /**
     * Gets the default read preference
     * @return
     */
    public ReadPreference getReadPreference(){
        return _readPref;
    }

    public DBCursor setDecoderFactory(DBDecoderFactory fact){
        _decoderFact = fact;
        return this;
    }

    public DBDecoderFactory getDecoderFactory(){
        return _decoderFact;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Cursor id=").append(getCursorId());
        sb.append(", ns=").append(getCollection().getFullName());
        sb.append(", query=").append(getQuery());
        if (getKeysWanted() != null)
            sb.append(", fields=").append(getKeysWanted());
        sb.append(", numIterated=").append(_num);
        if (_skip != 0)
            sb.append(", skip=").append(_skip);
        if (_limit != 0)
            sb.append(", limit=").append(_limit);
        if (_batchSize != 0)
            sb.append(", batchSize=").append(_batchSize);

        ServerAddress addr = getServerAddress();
        if (addr != null)
            sb.append(", addr=").append(addr);

        if (_readPref != null)
            sb.append(", readPreference=").append( _readPref.toString() );
        return sb.toString();
    }

    boolean hasFinalizer() {
        if (_it == null || ! (_it instanceof Result)) {
            return false;
        }
        return ((Result) _it).hasFinalizer();
    }

    // ----  query setup ----
    private final DBCollection _collection;
    private final DBObject _query;
    private final DBObject _keysWanted;

    private DBObject _orderBy = null;
    private String _hint = null;
    private DBObject _hintDBObj = null;
    private boolean _explain = false;
    private int _limit = 0;
    private int _batchSize = 0;
    private int _skip = 0;
    private boolean _snapshot = false;
    private int _options = 0;
    private ReadPreference _readPref;
    private DBDecoderFactory _decoderFact;

    private DBObject _specialFields;

    // ----  result info ----
    private Iterator<DBObject> _it = null;

    private CursorType _cursorType = null;
    private DBObject _cur = null;
    private int _num = 0;

    private final ArrayList<DBObject> _all = new ArrayList<DBObject>();
}
