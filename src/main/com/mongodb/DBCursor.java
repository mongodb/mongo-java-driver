/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb;

import org.bson.util.annotations.NotThreadSafe;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * <p>An iterator over database results. Doing a {@code find()} query on a collection returns a {@code DBCursor} thus</p>
 * <pre>
 * DBCursor cursor = collection.find( query );
 *    if(cursor.hasNext()) {
 *     DBObject obj = cursor.next();
 *    }
 * </pre>
 * <p><b>Warning:</b> Calling {@code toArray} or {@code length} on a DBCursor will irrevocably turn it into an array.  This means that, if
 * the cursor was iterating over ten million results (which it was lazily fetching from the database), suddenly there will be a ten-million
 * element array in memory.  Before converting to an array, make sure that there are a reasonable number of results using {@code skip()} and
 * {@code limit()}.
 *
 * <p>For example, to get an array of the 1000-1100th elements of a cursor, use</p>
 *
 * <pre>{@code
 * List<DBObject> obj = collection.find( query ).skip( 1000 ).limit( 100 ).toArray();
 * }</pre>
 *
 * @mongodb.driver.manual core/read-operations Read Operations
 */
@NotThreadSafe
public class DBCursor implements Cursor, Iterable<DBObject> {

    /**
     * Initializes a new database cursor.
     *
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
     * Adds a comment to the query to identify queries in the database profiler output.
     * 
     * @param comment the comment that is to appear in the profiler output
     * @return {@code this} so calls can be chained
     * @mongodb.driver.manual reference/operator/meta/comment/ $comment
     * @since 2.12
     */
    public DBCursor comment(final String comment) {
        addSpecial(QueryOperators.COMMENT, comment);
        return this;
    }

    /**
     * Limits the number of documents a cursor will return for a query.
     * 
     * @param max the maximum number of documents to return
     * @return {@code this} so calls can be chained
     * @mongodb.driver.manual reference/operator/meta/maxScan/ $maxScan
     * @see #limit(int) 
     * @since 2.12
     */
    public DBCursor maxScan(final int max) {
        addSpecial(QueryOperators.MAX_SCAN, max);
        return this;
    }

    /**
     * Specifies an <em>exclusive</em> upper limit for the index to use in a query.
     *
     * @param max a document specifying the fields, and the upper bound values for those fields
     * @return {@code this} so calls can be chained
     * @mongodb.driver.manual reference/operator/meta/max/ $max
     * @since 2.12
     */
    public DBCursor max(final DBObject max) {
        addSpecial(QueryOperators.MAX, max);
        return this;
    }

    /**
     * Specifies an <em>inclusive</em> lower limit for the index to use in a query. 
     *
     * @param min a document specifying the fields, and the lower bound values for those fields
     * @return {@code this} so calls can be chained
     * @mongodb.driver.manual reference/operator/meta/min/ $min
     * @since 2.12
     */
    public DBCursor min(final DBObject min) {
        addSpecial(QueryOperators.MIN, min);
        return this;
    }

    /**
     * Forces the cursor to only return fields included in the index.
     *
     * @return {@code this} so calls can be chained
     * @mongodb.driver.manual reference/operator/meta/returnKey/ $returnKey
     * @since 2.12
     */
    public DBCursor returnKey() {
        addSpecial(QueryOperators.RETURN_KEY, true);
        return this;
    }

    /**
     * Modifies the documents returned to include references to the on-disk location of each document.  The location will be returned in a
     * property named {@code $diskLoc}
     *
     * @return {@code this} so calls can be chained
     * @mongodb.driver.manual reference/operator/meta/showDiskLoc/ $showDiskLoc
     * @since 2.12
     */
    public DBCursor showDiskLoc() {
        addSpecial(QueryOperators.SHOW_DISK_LOC, true);
        return this;
    }

    /**
     * Types of cursors: iterator or array.
     */
    static enum CursorType { ITERATOR , ARRAY }

    /**
     * Creates a copy of an existing database cursor. The new cursor is an iterator, even if the original was an array.
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
        c._maxTimeMS = _maxTimeMS;
        c._disableBatchSizeTracking = _disableBatchSizeTracking;
        if ( _specialFields != null )
            c._specialFields = new BasicDBObject( _specialFields.toMap() );
        return c;
    }

    /**
     * <p>Creates a copy of this cursor object that can be iterated. Note: - you can iterate the DBCursor itself without calling this method
     * - no actual data is getting copied.</p>
     *
     * <p>Note that use of this method does not let you call close the underlying cursor in the case of either an exception or an early
     * break.  The preferred method of iteration is to use DBCursor as an Iterator, so that you can call close() on it in a finally
     * block.</p>
     *
     * @return an iterator
     */
    @Override
    public Iterator<DBObject> iterator(){
        return this.copy();
    }

    // ---- querty modifiers --------

    /**
     * Sorts this cursor's elements. This method must be called before getting any object from the cursor.
     *
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
     * Adds a special operator like $maxScan or $returnKey. For example:
     * <pre>
     *    addSpecial("$returnKey", 1)
     *    addSpecial("$maxScan", 100)
     * </pre>
     *
     * @param name the name of the special query operator
     * @param o    the value of the special query operator
     * @return {@code this} so calls can be chained
     * @mongodb.driver.manual reference/operator Special Operators
     */
    public DBCursor addSpecial(String name, Object o) {
        if ( _specialFields == null )
            _specialFields = new BasicDBObject();
        _specialFields.put( name , o );
        return this;
    }

    /**
     * Informs the database of indexed fields of the collection in order to improve performance.
     *
     * @param indexKeys a {@code DBObject} with fields and direction
     * @return same DBCursor for chaining operations
     * @mongodb.driver.manual reference/operator/meta/hint/ $hint
     */
    public DBCursor hint(final DBObject indexKeys) {
        if ( _it != null )
            throw new IllegalStateException( "can't hint after executing query" );
        
        _hintDBObj = indexKeys;
        return this;
    }

    /**
     * Informs the database of an indexed field of the collection in order to improve performance.
     *
     * @param indexName the name of an index
     * @return same DBCursor for chaining operations
     * @mongodb.driver.manual reference/operator/meta/hint/ $hint
     */
    public DBCursor hint(final String indexName) {
        if ( _it != null )
            throw new IllegalStateException( "can't hint after executing query" );

        _hint = indexName;
        return this;
    }

    /**
     * Set the maximum execution time for operations on this cursor.
     *
     * @param maxTime  the maximum time that the server will allow the query to run, before killing the operation. A non-zero value requires
     *                 a server version &gt;= 2.6
     * @param timeUnit the time unit
     * @return same DBCursor for chaining operations
     * @mongodb.server.release 2.6
     * @mongodb.driver.manual reference/operator/meta/maxTimeMS/ $maxTimeMS
     * @since 2.12.0
     */
    public DBCursor maxTime(final long maxTime, final TimeUnit timeUnit) {
        _maxTimeMS = MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    /**
     * Use snapshot mode for the query. Snapshot mode prevents the cursor from returning a document more than once because an intervening
     * write operation results in a move of the document. Even in snapshot mode, documents inserted or deleted during the lifetime of the
     * cursor may or may not be returned.  Currently, snapshot mode may not be used with sorting or explicit hints.
     *
     * @return {@code this} so calls can be chained
     *
     * @see com.mongodb.DBCursor#sort(DBObject)
     * @see com.mongodb.DBCursor#hint(DBObject)
     * @mongodb.driver.manual reference/operator/meta/snapshot/ $snapshot
     */
    public DBCursor snapshot() {
        if (_it != null)
            throw new IllegalStateException("can't snapshot after executing the query");

        _snapshot = true;

        return this;
    }

    /**
     * Returns an object containing basic information about the execution of the query that created this cursor. This creates a {@code
     * DBObject} with a number of fields, including but not limited to: 
     * <ul>
     *     <li><i>cursor:</i> cursor type</li>
     *     <li><i>nScanned:</i> number of records examined by the database for this query </li>
     *     <li><i>n:</i> the number of records that the database returned</li>
     *     <li><i>millis:</i> how long it took the database to execute the query</li>
     * </ul>
     *
     * @return a {@code DBObject} containing the explain output for this DBCursor's query
     * @throws MongoException
     * @mongodb.driver.manual reference/explain Explain Output
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
     * Limits the number of elements returned. Note: parameter {@code limit} should be positive, although a negative value is
     * supported for legacy reason. Passing a negative value will call {@link DBCursor#batchSize(int)} which is the preferred method.
     *
     * @param limit the number of elements to return
     * @return a cursor to iterate the results
     * @mongodb.driver.manual reference/method/cursor.limit Limit
     */
    public DBCursor limit(final int limit) {
        if ( _it != null )
            throw new IllegalStateException( "can't set limit after executing query" );

        if (limit > 0)
            _limit = limit;
        else if (limit < 0)
            batchSize(limit);
        return this;
    }

    /**
     * <p>Limits the number of elements returned in one batch. A cursor typically fetches a batch of result objects and store them
     * locally.</p>
     *
     * <p>If {@code batchSize} is positive, it represents the size of each batch of objects retrieved. It can be adjusted to optimize
     * performance and limit data transfer.</p>
     *
     * <p>If {@code batchSize} is negative, it will limit of number objects returned, that fit within the max batch size limit (usually
     * 4MB), and cursor will be closed. For example if {@code batchSize} is -10, then the server will return a maximum of 10 documents and
     * as many as can fit in 4MB, then close the cursor. Note that this feature is different from limit() in that documents must fit within
     * a maximum size, and it removes the need to send a request to close the cursor server-side.</p>
     *
     * @param numberOfElements the number of elements to return in a batch
     * @return {@code this} so calls can be chained
     */
    public DBCursor batchSize(int numberOfElements) {
        // check for special case, used to have server bug with 1
        if ( numberOfElements == 1 )
            numberOfElements = 2;

        if ( _it != null ) {
            _it.setBatchSize(numberOfElements);
        }

        _batchSize = numberOfElements;
        return this;
    }

    /**
     * Discards a given number of elements at the beginning of the cursor.
     *
     * @param numberOfElements the number of elements to skip
     * @return a cursor pointing to the new first element of the results
     * @throws IllegalStateException if the cursor has started to be iterated through
     */
    public DBCursor skip(final int numberOfElements) {
        if ( _it != null )
            throw new IllegalStateException( "can't set skip after executing query" );
        _skip = numberOfElements;
        return this;
    }

    @Override
    public long getCursorId() {
    	return _it == null ? 0 : _it.getCursorId();
    }

    @Override
    public void close() {
    	if (_it != null)
            _it.close();
    }

    /**
     * Declare that this query can run on a secondary server.
     *
     * @return a copy of the same cursor (for chaining)
     * @see ReadPreference#secondaryPreferred()
     * @deprecated Replaced with {@link com.mongodb.ReadPreference#secondaryPreferred()}
     * @see ReadPreference#secondaryPreferred()
     */
    @Deprecated
    public DBCursor slaveOk(){
        return addOption( Bytes.QUERYOPTION_SLAVEOK );
    }

    /**
     * Adds a query option. See Bytes.QUERYOPTION_* for list.
     *
     * @param option the option to be added
     * @return {@code this} so calls can be chained
     * @see Bytes
     * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query Query Flags
     */
    public DBCursor addOption(final int option) {
        setOptions(_options |= option);

        return this;
    }

    /**
     * Sets the query option - see Bytes.QUERYOPTION_* for list.
     *
     * @param options the bitmask of options
     * @return {@code this} so calls can be chained
     * @see Bytes
     * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query Query Flags
     */
    public DBCursor setOptions( int options ){
        if ((options & Bytes.QUERYOPTION_EXHAUST) != 0) {
            throw new IllegalArgumentException("The exhaust option is not user settable.");
        }

        _options = options;
        return this;
    }

    /**
     * Resets the query options.
     *
     * @return {@code this} so calls can be chained
     * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query Query Flags
     */
    public DBCursor resetOptions(){
        _options = 0;
        return this;
    }

    /**
     * Gets the query options.
     *
     * @return the bitmask of options
     * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query Query Flags
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
                .addSpecialFields(_specialFields)
                .addMaxTimeMS(_maxTimeMS);

        if (_collection.getDB().getMongo().isMongosConnection()) {
            builder.addReadPreference(_readPref);
        }

        _it = _collection.find(builder.get(), _keysWanted, _skip, _batchSize, _limit, _options, _readPref, getDecoder());
        if (_disableBatchSizeTracking) {
            _it.disableBatchSizeTracking();
        }
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
     * Gets the number of times, so far, that the cursor retrieved a batch from the database
     *
     * @return The number of times OP_GET_MORE has been called
     * @deprecated there is no replacement for this method
     */
    @Deprecated
    public int numGetMores() {
        return _it == null ? 0 : _it.numGetMores();
    }

    /**
     * Gets a list containing the number of items received in each batch
     *
     * @return a list containing the number of items received in each batch
     * @deprecated there is no replacement for this method
     */
    @Deprecated
    public List<Integer> getSizes() {
        return _it == null ? Collections.<Integer>emptyList() : _it.getSizes();
    }

    /**
     * Disable tracking of batch sizes in order to reduce memory consumption with high-frequency tailable cursors. This method must be
     * called before iterating the cursor.
     *
     * @return this
     * @see #getSizes()
     * @deprecated This method is being added temporarily, and will be removed along with {@link #getSizes()} in the next major release
     */
    @Deprecated
    public DBCursor disableBatchSizeTracking() {
        if ( _it != null )
            throw new IllegalStateException( "can't disable batch size tracking after executing query" );

        _disableBatchSizeTracking = true;
        return this;
    }

    /**
     * Returns true if tracking of batch sizes is disabled in order to reduce memory consumption with high-frequency tailable cursors.
     *
     * @return true if tracking of batch sizes is disabled
     * @see #getSizes()
     * @see #disableBatchSizeTracking()
     * @deprecated This method is being added temporarily, and will be removed along with {@link #getSizes()} in the next major release
     */
    @Deprecated
    public boolean isBatchSizeTrackingDisabled() {
        return _disableBatchSizeTracking;
    }

    private boolean _hasNext() {
        _check();

        if ( _limit > 0 && _num >= _limit )
            return false;

        return _it.hasNext();
    }

    /**
     * Returns the number of objects through which the cursor has iterated.
     *
     * @return the number of objects seen
     */
    public int numSeen(){
        return _num;
    }

    // ----- iterator api -----

    /**
     * Checks if there is another object available.
     *
     * <p><em>Note</em>: Automatically adds the {@link Bytes#QUERYOPTION_AWAITDATA} option to any cursors with the
     * {@link Bytes#QUERYOPTION_TAILABLE} option set. For non blocking tailable cursors see {@link #tryNext }.</p>
     *
     * @return true if there is another object available
     * @mongodb.driver.manual /core/cursors/#cursor-batches Cursor Batches
     * @throws MongoException
     */
    public boolean hasNext() {
        _checkType(CursorType.ITERATOR);

        if ((getOptions() & Bytes.QUERYOPTION_TAILABLE) != 0) {
            addOption(Bytes.QUERYOPTION_AWAITDATA);
        }

        return _hasNext();
    }

    /**
     * Non blocking check for tailable cursors to see if another object is available.
     *
     * <p>Returns the object the cursor is at and moves the cursor ahead by one or
     * return null if no documents is available.</p>
     *
     * @return the next element or null
     * @throws MongoException
     * @mongodb.driver.manual /core/cursors/#cursor-batches Cursor Batches
     */
    public DBObject tryNext() {
        _checkType( CursorType.ITERATOR );

        if ((getOptions() & Bytes.QUERYOPTION_TAILABLE) != Bytes.QUERYOPTION_TAILABLE) {
            throw new IllegalArgumentException("Can only be used with a tailable cursor");
        }

        _check();

        if (!_it.tryHasNext()) {
            return null;
        }
        return _next();
    }

    /**
     * Returns the object the cursor is at and moves the cursor ahead by one.
     *
     * <p><em>Note</em>: Automatically adds the {@link Bytes#QUERYOPTION_AWAITDATA} option to any cursors with the
     * {@link Bytes#QUERYOPTION_TAILABLE} option set. For non blocking tailable cursors see {@link #tryNext }.</p>
     *
     * @return the next element
     * @mongodb.driver.manual /core/cursors/#cursor-batches Cursor Batches
     */
    public DBObject next() {
        _checkType( CursorType.ITERATOR );

        if ((getOptions() & Bytes.QUERYOPTION_TAILABLE) != 0) {
            addOption(Bytes.QUERYOPTION_AWAITDATA);
        }

        return _next();
    }

    /**
     * Returns the element the cursor is at.
     *
     * @return the current element
     */
    public DBObject curr(){
        _checkType(CursorType.ITERATOR);
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
     * Pulls back all items into an array and returns the number of objects. Note: this can be resource intensive.
     *
     * @return the number of elements in the array
     * @throws MongoException
     * @see #count()
     * @see #size()
     */
    public int length() {
        _checkType( CursorType.ARRAY );
        _fill( Integer.MAX_VALUE );
        return _all.size();
    }

    /**
     * Converts this cursor to an array.
     *
     * @return an array of elements
     * @throws MongoException
     */
    public List<DBObject> toArray(){
        return toArray( Integer.MAX_VALUE );
    }

    /**
     * Converts this cursor to an array.
     *
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
     * For testing only! Iterates cursor and counts objects
     *
     * @return num objects
     * @throws MongoException
     * @see #count()
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
     * Counts the number of objects matching the query. This does not take limit/skip into consideration, and does initiate a call to the
     * server.
     *
     * @return the number of objects
     * @throws MongoException
     * @see DBCursor#size
     */
    public int count() {
        Object hint = _hint != null ? _hint : _hintDBObj;
        if (hint == null && _specialFields != null && _specialFields.containsField("$hint")) {
            hint = _specialFields.get("$hint");
        }
        return (int) _collection.getCount(this._query, this._keysWanted, 0, 0, getReadPreference(), _maxTimeMS,
                                         MILLISECONDS, hint);
    }

    /**
     * Returns the first document that matches the query.
     *
     * @return the first matching document
     * @since 2.12
     */
    public DBObject one() {
        return _collection.findOne(_query, _keysWanted, _orderBy, getReadPreference(), _maxTimeMS, MILLISECONDS);
    }


    /**
     * Counts the number of objects matching the query this does take limit/skip into consideration
     *
     * @return the number of objects
     * @throws MongoException
     * @see #count()
     */
    public int size() {
        return (int)_collection.getCount(this._query, this._keysWanted, this._limit, this._skip, getReadPreference(), _maxTimeMS,
                                         MILLISECONDS);
    }


    /**
     * Gets the fields to be returned.
     *
     * @return the field selector that cursor used
     */
    public DBObject getKeysWanted(){
        return _keysWanted;
    }

    /**
     * Gets the query.
     *
     * @return the query that cursor used
     */
    public DBObject getQuery(){
        return _query;
    }

    /**
     * Gets the collection.
     *
     * @return the collection that data is pulled from
     */
    public DBCollection getCollection(){
        return _collection;
    }

    /**
     * Gets the Server Address of the server that data is pulled from. Note that this information may not be available until hasNext() or
     * next() is called.
     *
     * @return the address of the server
     */
    public ServerAddress getServerAddress() {
        return _it == null ? null : _it.getServerAddress();
    }

    /**
     * Sets the read preference for this cursor. See the documentation for {@link ReadPreference} for more information.
     *
     * @param readPreference read preference to use
     * @return {@code this} so calls can be chained
     */
    public DBCursor setReadPreference(final ReadPreference readPreference) {
        _readPref = readPreference;
        return this;
    }

    /**
     * Gets the default read preference.
     *
     * @return the readPreference used by this cursor
     */
    public ReadPreference getReadPreference(){
        return _readPref;
    }

    /**
     * Sets the factory that will be used create a {@code DBDecoder} that will be used to decode BSON documents into DBObject instances.
     *
     * @param fact the DBDecoderFactory
     * @return {@code this} so calls can be chained
     */
    public DBCursor setDecoderFactory(final DBDecoderFactory fact) {
        _decoderFact = fact;
        return this;
    }

    /**
     * Gets the decoder factory that creates the decoder this cursor will use to decode objects from MongoDB. 
     *
     * @return the decoder factory.
     */
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
        if (_it == null) {
            return false;
        }
        return _it.hasFinalizer();
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
    private long _maxTimeMS;
    private ReadPreference _readPref;
    private DBDecoderFactory _decoderFact;

    private DBObject _specialFields;

    // ----  result info ----
    private QueryResultIterator _it = null;

    private CursorType _cursorType = null;
    private DBObject _cur = null;
    private int _num = 0;
    private boolean _disableBatchSizeTracking;

    private final ArrayList<DBObject> _all = new ArrayList<DBObject>();
}
