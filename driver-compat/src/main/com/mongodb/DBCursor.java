/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

import com.mongodb.codecs.DBDecoderAdapter;
import com.mongodb.codecs.DBObjectCodec;
import org.mongodb.Decoder;
import org.mongodb.MongoQueryCursor;
import org.mongodb.annotations.NotThreadSafe;
import org.mongodb.operation.Find;
import org.mongodb.operation.QueryFlag;
import org.mongodb.operation.QueryOperation;
import org.mongodb.session.Session;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static com.mongodb.DBObjects.toDocument;
import static com.mongodb.MongoExceptions.mapException;

/**
 * An iterator over database results. Doing a <code>find()</code> query on a collection returns a {@code DBCursor}
 * thus
 * <p/>
 * <blockquote><pre>
 * DBCursor cursor = collection.find( query );
 * if( cursor.hasNext() )
 *     DBObject obj = cursor.next();
 * </pre></blockquote>
 * <p/>
 * <p><b>Warning:</b> Calling <code>toArray</code> or {@code length} on a DBCursor will irrevocably turn it into an
 * array.  This means that, if the cursor was iterating over ten million results (which it was lazily fetching from the
 * database), suddenly there will be a ten-million element array in memory.  Before converting to an array, make sure
 * that there are a reasonable number of results using <code>skip()</code> and {@code limit()}. <p>For example, to
 * get an array of the 1000-1100th elements of a cursor, use
 * <p/>
 * <blockquote><pre>
 * List<DBObject> obj = collection.find( query ).skip( 1000 ).limit( 100 ).toArray();
 * </pre></blockquote>
 *
 * @mongodb.driver.manual core/read-operations Read Operations
 */
@NotThreadSafe
public class DBCursor implements Iterator<DBObject>, Iterable<DBObject>, Closeable {
    private final DBCollection collection;
    private final Find find;
    private Decoder<DBObject> resultDecoder;
    private DBDecoderFactory decoderFactory;
    private CursorType cursorType;
    private DBObject currentObject;
    private int numSeen;
    private boolean closed;
    private final List<DBObject> all = new ArrayList<DBObject>();
    private MongoQueryCursor<DBObject> cursor;

    /**
     * Initializes a new database cursor
     *
     * @param collection     collection to use
     * @param query          query to perform
     * @param fields         keys to return from the query
     * @param readPreference the read preference for this query
     */
    public DBCursor(final DBCollection collection, final DBObject query, final DBObject fields, final ReadPreference readPreference) {
        this(collection,
                new Find()
                        .where(toDocument(query))
                        .select(DBObjects.toFieldSelectorDocument(fields))
                        .readPreference(readPreference.toNew())
                        .addFlags(QueryFlag.toSet(collection.getOptions())));
    }

    private DBCursor(final DBCollection collection, final Find find) {
        if (collection == null) {
            throw new IllegalArgumentException("Collection can't be null");
        }
        this.collection = collection;
        this.find = new Find(find);
        if (collection.getDBDecoderFactory() != null) {
            setDecoderFactory(collection.getDBDecoderFactory());
        } else {
            this.resultDecoder = collection.getObjectCodec();
        }
    }

    /**
     * Creates a copy of an existing database cursor. The new cursor is an iterator, even if the original was an array.
     *
     * @return the new cursor
     */
    public DBCursor copy() {
        return new DBCursor(collection, find);
    }

    public boolean hasNext() {

        if (closed) {
            throw new IllegalStateException("Cursor has been closed");
        }

        if (cursor == null) {
            try {
                cursor = new MongoQueryCursor<DBObject>(collection.getNamespace(), find, collection.getDocumentCodec(), resultDecoder,
                        getCollection().getBufferPool(), getSession(), true);
            } catch (org.mongodb.MongoException e) {
                throw mapException(e);
            }
            //        sizes.add(results.size());
        }

        return cursor.hasNext();
    }

    public DBObject next() {
        checkCursorType(CursorType.ITERATOR);
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        return nextInternal();
    }

    /**
     * Returns the element the cursor is at.
     *
     * @return the next element
     */
    public DBObject curr() {
        return currentObject;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    /**
     * adds a query option - see Bytes.QUERYOPTION_* for list
     *
     * @param option
     * @return
     */
    public DBCursor addOption(final int option) {
        find.addFlags(QueryFlag.toSet(option));
        return this;
    }

    /**
     * sets the query option - see Bytes.QUERYOPTION_* for list
     *
     * @param options
     */
    public DBCursor setOptions(final int options) {
        find.flags(QueryFlag.toSet(options));
        return this;
    }

    /**
     * resets the query options
     */
    public DBCursor resetOptions() {
        find.flags(QueryFlag.toSet(collection.getOptions()));
        return this;
    }

    /**
     * gets the query options
     *
     * @return
     */
    public int getOptions() {
        return QueryFlag.fromSet(find.getOptions().getFlags());
    }

    /**
     * adds a special operator like $maxScan or $returnKey e.g. addSpecial( "$returnKey" , 1 ) e.g. addSpecial(
     * "$maxScan" , 100 )
     *
     * @param name the name of the special query operator
     * @param o    the value of the special query operator
     * @return this
     * @mongodb.driver.manual reference/operator Special Operators
     */
    public DBCursor addSpecial(final String name, final Object o) {
        throw new UnsupportedOperationException();  // TODO
    }

    /**
     * Informs the database of indexed fields of the collection in order to improve performance.
     *
     * @param indexKeys a {@code DBObject} with fields and direction
     * @return same DBCursor for chaining operations
     */
    public DBCursor hint(final DBObject indexKeys) {
        throw new UnsupportedOperationException();  // TODO
    }

    /**
     * Informs the database of an indexed field of the collection in order to improve performance.
     *
     * @param indexName the name of an index
     * @return same DBCursort for chaining operations
     */
    public DBCursor hint(final String indexName) {
        find.hintIndex(indexName);
        return this;
    }

    /**
     * Use snapshot mode for the query. Snapshot mode assures no duplicates are returned, or objects missed, which were
     * present at both the start and end of the query's execution (if an object is new during the query, or deleted
     * during the query, it may or may not be returned, even with snapshot mode). Note that short query responses (less
     * than 1MB) are always effectively snapshot. Currently, snapshot mode may not be used with sorting or explicit
     * hints.
     *
     * @return this
     */
    public DBCursor snapshot() {
        find.snapshot();
        return this;
    }

    /**
     * Returns an object containing basic information about the execution of the query that created this cursor This
     * creates a {@code DBObject} with a number of fields, including but not limited to:
     * cursor : cursor type
     * nScanned number of records examined by the database for this query
     * n : the number of records that the database returned
     * millis : how long it took the database to execute the query
     *
     * @return a {@code DBObject} containing the explain output for this DBCursor's query
     * @throws MongoException
     * @mongodb.driver.manual reference/explain Explain Output
     */
    public DBObject explain() {
        final Find copy = new Find(find);
        copy.explain();
        if (copy.getLimit() > 0) {
            // need to pass a negative batchSize as limit for explain
            copy.batchSize(copy.getLimit() * -1);
            copy.limit(0);
        }
        try {
            return new QueryOperation<DBObject>(collection.getNamespace(), copy, collection.getDocumentCodec(), new DBObjectCodec(),
                            getCollection().getBufferPool(), getSession(), true).execute().next();
        } catch (org.mongodb.MongoException e) {
            throw mapException(e);
        }
    }

    /**
     * Sorts this cursor's elements. This method must be called before getting any object from the cursor.
     *
     * @param orderBy the fields by which to sort
     * @return a cursor pointing to the first element of the sorted results
     */
    public DBCursor sort(final DBObject orderBy) {
        find.order(toDocument(orderBy));
        return this;
    }

    /**
     * Limits the number of elements returned. Note: parameter <tt>n</tt> should be positive, although a negative value
     * is supported for legacy reason. Passing a negative value will call {@link DBCursor#batchSize(int)} which is the
     * preferred method.
     *
     * @param n the number of elements to return
     * @return a cursor to iterate the results
     * @mongodb.driver.manual reference/method/cursor.limit Limit
     */
    public DBCursor limit(final int n) {
        find.limit(n);
        return this;
    }

    /**
     * Limits the number of elements returned in one batch. A cursor typically fetches a batch of result objects and
     * store them locally.
     * <p/>
     * If <tt>batchSize</tt> is positive, it represents the size of each batch of objects retrieved. It can be adjusted
     * to optimize performance and limit data transfer.
     * <p/>
     * If <tt>batchSize</tt> is negative, it will limit of number objects returned, that fit within the max batch size
     * limit (usually 4MB), and cursor will be closed. For example if <tt>batchSize</tt> is -10, then the server will
     * return a maximum of 10 documents and as many as can fit in 4MB, then close the cursor. Note that this feature is
     * different from limit() in that documents must fit within a maximum size, and it removes the need to send a
     * request to close the cursor server-side.
     * <p/>
     * The batch size can be changed even after a cursor is iterated, in which case the setting will apply on the next
     * batch retrieval.
     *
     * @param n the number of elements to return in a batch
     * @return
     */
    public DBCursor batchSize(final int n) {
        find.getOptions().batchSize(n);
        return this;
    }

    /**
     * Discards a given number of elements at the beginning of the cursor.
     *
     * @param n the number of elements to skip
     * @return a cursor pointing to the new first element of the results
     * @throws IllegalStateException if the cursor has started to be iterated through
     */
    public DBCursor skip(final int n) {
        find.skip(n);
        return this;
    }

    /**
     * gets the cursor id.
     *
     * @return the cursor id, or 0 if there is no active cursor.
     */
    public long getCursorId() {
        if (cursor != null) {
            return cursor.getServerCursor().getId();
        } else {
            return 0;
        }
    }

    /**
     * Gets the number of times, so far, that the cursor retrieved a batch from the database
     *
     * @return the number of get more operations
     */
    public int numGetMores() {
        return cursor != null ? cursor.getNumGetMores() : 0;
    }

    /**
     * Gets a list containing the number of items received in each batch
     *
     * @return the size of each batch
     */
    public List<Integer> getSizes() {
        return cursor != null ? cursor.getSizes() : Collections.<Integer>emptyList();
    }

    /**
     * Returns the number of objects through which the cursor has iterated.
     *
     * @return the number of objects seen
     */
    public int numSeen() {
        return numSeen;
    }

    /**
     * kills the current cursor on the server.
     */
    @Override
    public void close() {
        closed = true;
        if (cursor != null) {
            cursor.close();
        }

        currentObject = null;
    }

    /**
     * makes this query ok to run on a slave node
     *
     * @return a copy of the same cursor (for chaining)
     * @see ReadPreference#secondaryPreferred()
     * @deprecated Replaced with {@code ReadPreference.secondaryPreferred()}
     */
    @Deprecated
    public DBCursor slaveOk() {
        return addOption(Bytes.QUERYOPTION_SLAVEOK);
    }

    /**
     * creates a copy of this cursor object that can be iterated. Note: - you can iterate the DBCursor itself without
     * calling this method - no actual data is getting copied.
     *
     * @return an iterator
     */
    // TODO: document the dangerousness of this method, regarding close...
    public Iterator<DBObject> iterator() {
        return this.copy();
    }

    /**
     * Converts this cursor to an array.
     *
     * @return an array of elements
     * @throws MongoException
     */
    public List<DBObject> toArray() {
        return toArray(Integer.MAX_VALUE);
    }

    /**
     * Converts this cursor to an array.
     *
     * @param max the maximum number of objects to return
     * @return an array of objects
     * @throws MongoException
     */
    public List<DBObject> toArray(final int max) {
        checkCursorType(CursorType.ARRAY);
        fillArray(max - 1);
        return all;
    }

    /**
     * Counts the number of objects matching the query This does not take limit/skip into consideration
     *
     * @return the number of objects
     * @throws MongoException
     * @see DBCursor#size
     */
    public int count() {
        return (int) collection.count(getQuery(), getReadPreference());
        // TODO: dangerous cast.  Throw exception instead?
    }

    /**
     * pulls back all items into an array and returns the number of objects. Note: this can be resource intensive
     *
     * @return the number of elements in the array
     * @throws MongoException
     * @see #count()
     * @see #size()
     */
    public int length() {
        checkCursorType(CursorType.ARRAY);
        fillArray(Integer.MAX_VALUE);
        return all.size();
    }

    /**
     * for testing only! Iterates cursor and counts objects
     *
     * @return num objects
     * @throws MongoException
     * @see #count()
     */
    public int itcount() {
        int n = 0;
        while (this.hasNext()) {
            this.next();
            n++;
        }
        return n;
    }


    /**
     * Counts the number of objects matching the query this does take limit/skip into consideration
     *
     * @return the number of objects
     * @throws MongoException
     * @see #count()
     */
    public int size() {
        return (int) collection.getCount(getQuery(), getKeysWanted(), find.getLimit(), find.getSkip(), getReadPreference());
        // TODO: dangerous cast.  Throw exception instead?
    }

    /**
     * gets the fields to be returned
     *
     * @return
     */
    public DBObject getKeysWanted() {
        if (find.getFields() == null) {
            return null;
        }
        return DBObjects.toDBObject(find.getFields());
    }

    /**
     * gets the query
     *
     * @return
     */
    public DBObject getQuery() {
        return DBObjects.toDBObject(find.getFilter());
    }

    /**
     * gets the collection
     *
     * @return
     */
    public DBCollection getCollection() {
        return collection;
    }

    /**
     * Gets the Server Address of the server that data is pulled from. Note that this information may not be available
     * until hasNext() or next() is called.
     *
     * @return
     */
    public ServerAddress getServerAddress() {
        if (cursor != null) {
            return new ServerAddress(cursor.getServerAddress());
        } else {
            return null;
        }
    }

    /**
     * Sets the read preference for this cursor. See the * documentation for {@link ReadPreference} for more
     * information.
     *
     * @param readPreference read preference to use
     */
    public DBCursor setReadPreference(final ReadPreference readPreference) {
        find.readPreference(readPreference.toNew());
        return this;
    }

    /**
     * Gets the default read preference
     *
     * @return
     */
    public ReadPreference getReadPreference() {
        return ReadPreference.fromNew(find.getReadPreference());
    }

    public DBCursor setDecoderFactory(final DBDecoderFactory factory) {
        this.decoderFactory = factory;
        this.resultDecoder = new DBDecoderAdapter(factory.create(), collection, getCollection().getBufferPool());
        return this;
    }

    public DBDecoderFactory getDecoderFactory() {
        return decoderFactory;
    }

    @Override
    public String toString() {
        return "DBCursor{" +
                "collection=" + collection +
                ", find=" + find +
                (cursor != null ? (", cursor=" + cursor.getServerCursor()) : "") +
                '}';
    }

    private void checkCursorType(final CursorType type) {
        if (cursorType == null) {
            cursorType = type;
            return;
        }

        if (type == cursorType) {
            return;
        }

        throw new IllegalArgumentException("Can't switch cursor access methods");
    }

    private void fillArray(final int n) {
        checkCursorType(CursorType.ARRAY);
        while (n >= all.size() && hasNext()) {
            all.add(nextInternal());
        }
    }

    private DBObject nextInternal() {
        if (cursorType == null) {
            checkCursorType(CursorType.ITERATOR);
        }

        currentObject = cursor.next();
        numSeen++;

        if (find.getFields() != null && !find.getFields().isEmpty()) {
            currentObject.markAsPartialObject();
        }

        return currentObject;
    }

    public Session getSession() {
        return getCollection().getSession();
    }

    /**
     * Types of cursors: iterator or array.
     */
    private static enum CursorType {
        ITERATOR,
        ARRAY
    }
}