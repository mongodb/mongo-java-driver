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

import com.mongodb.annotations.NotThreadSafe;
import com.mongodb.client.model.FindModel;
import com.mongodb.operation.QueryOperation;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.codecs.Decoder;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * <p>An iterator over database results. Doing a {@code find()} query on a collection returns a {@code DBCursor} thus</p>
 * <pre>
 *    DBCursor cursor = collection.find(query);
 *    if(cursor.hasNext()) {
 *       DBObject obj = cursor.next();
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
 *    List<DBObject> obj = collection.find(query).skip(1000).limit(100).toArray();
 * }</pre>
 *
 * @mongodb.driver.manual core/read-operations Read Operations
 */
@NotThreadSafe
public class DBCursor implements Cursor, Iterable<DBObject> {
    private final DBCollection collection;
    private final FindModel<BsonDocument> findModel;
    private ReadPreference readPreference;
    private Decoder<DBObject> resultDecoder;
    private DBDecoderFactory decoderFactory;
    private CursorType cursorType;
    private DBObject currentObject;
    private int numSeen;
    private boolean closed;
    private final List<DBObject> all = new ArrayList<DBObject>();
    private MongoTailableCursor<DBObject> cursor;
    // This allows us to easily enable/disable finalizer for cleaning up un-closed cursors
    @SuppressWarnings("UnusedDeclaration")// IDEs will say it can be converted to a local variable, resist the urge
    private final OptionalFinalizer optionalFinalizer;


    /**
     * Initializes a new database cursor.
     *
     * @param collection     collection to use
     * @param query          query to perform
     * @param fields         keys to return from the query
     * @param readPreference the read preference for this query
     */
    public DBCursor(final DBCollection collection, final DBObject query, final DBObject fields, final ReadPreference readPreference) {
        this(collection,
             new FindModel<BsonDocument>()
             .modifiers(new BsonDocument())
             .criteria(collection.wrapAllowNull(query))
             .projection(collection.wrapAllowNull(fields))
             .cursorFlags(CursorFlag.toSet(collection.getOptions())),
             readPreference
            );

        DBObject indexKeys = lookupSuitableHints(query, collection.getHintFields());
        if (indexKeys != null) {
            hint(indexKeys);
        }
    }

    private DBCursor(final DBCollection collection, final FindModel<BsonDocument> findModel, final ReadPreference readPreference) {
        if (collection == null) {
            throw new IllegalArgumentException("Collection can't be null");
        }
        this.collection = collection;
        this.findModel = findModel;
        this.readPreference = readPreference;
        this.resultDecoder = collection.getObjectCodec();
        this.decoderFactory = collection.getDBDecoderFactory();
        optionalFinalizer = collection.getDB().getMongo().getMongoClientOptions().isCursorFinalizerEnabled()
                            ? new OptionalFinalizer() : null;
    }

    /**
     * Creates a copy of an existing database cursor. The new cursor is an iterator, even if the original was an array.
     *
     * @return the new cursor
     */
    public DBCursor copy() {
        return new DBCursor(collection, new FindModel<BsonDocument>(findModel), readPreference);
    }

    /**
     * Checks if there is another object available.
     *
     * <p><em>Note</em>: Automatically adds the {@link Bytes#QUERYOPTION_AWAITDATA} option to any cursors with the
     * {@link Bytes#QUERYOPTION_TAILABLE} option set. For non blocking tailable cursors see {@link #tryNext }.</p>
     *
     * @return true if there is another object available
     */
    @Override
    public boolean hasNext() {
        if (closed) {
            throw new IllegalStateException("Cursor has been closed");
        }

        if (cursor == null) {
            cursor = collection.execute(getQueryOperation(findModel, resultDecoder), getReadPreference());
        }

        return cursor.hasNext();
    }

    /**
     * Returns the object the cursor is at and moves the cursor ahead by one.
     *
     * <p><em>Note</em>: Automatically adds the {@link Bytes#QUERYOPTION_AWAITDATA} option to any cursors with the
     * {@link Bytes#QUERYOPTION_TAILABLE} option set. For non blocking tailable cursors see {@link #tryNext }.</p>
     *
     * @return the next element
     */
    @Override
    public DBObject next() {
        checkCursorType(CursorType.ITERATOR);
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        return nextInternal();
    }

    /**
     * Non blocking check for tailable cursors to see if another object is available.
     *
     * <p>Returns the object the cursor is at and moves the cursor ahead by one or
     * return null if no documents is available.</p>
     *
     * @return the next element or null
     * @throws MongoException
     */
    public DBObject tryNext() {
        if (!findModel.getCursorFlags().contains(CursorFlag.Tailable)) {
            throw new IllegalArgumentException("Can only be used with a tailable cursor");
        }

        if (cursor == null) {
            cursor = collection.execute(getQueryOperation(findModel, resultDecoder), getReadPreference());
        }

        return cursor.tryNext();
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
     * Adds a query option. See Bytes.QUERYOPTION_* for list.
     *
     * @param option the option to be added
     * @return {@code this} so calls can be chained
     * @see Bytes
     */
    public DBCursor addOption(final int option) {
        findModel.getCursorFlags().addAll(CursorFlag.toSet(option));
        return this;
    }

    /**
     * Sets the query option - see Bytes.QUERYOPTION_* for list.
     *
     * @param options the bitmask of options
     * @return {@code this} so calls can be chained
     * @see Bytes
     */
    public DBCursor setOptions(final int options) {
        findModel.cursorFlags(CursorFlag.toSet(options));
        return this;
    }

    /**
     * Resets the query options.
     *
     * @return {@code this} so calls can be chained
     */
    public DBCursor resetOptions() {
        findModel.getCursorFlags().clear();
        return this;
    }

    /**
     * Gets the query options.
     *
     * @return the bitmask of options
     */
    public int getOptions() {
        return CursorFlag.fromSet(findModel.getCursorFlags());
    }

    /**
     * Adds a special operator like $maxScan or $returnKey. For example:
     * <pre>
     *    addSpecial("$returnKey", 1)
     *    addSpecial("$maxScan", 100)
     * </pre>
     *
     * @param name  the name of the special query operator
     * @param value the value of the special query operator
     * @return {@code this} so calls can be chained
     * @mongodb.driver.manual reference/operator Special Operators
     */
    public DBCursor addSpecial(final String name, final Object value) {
        if (name == null || value == null) {
            return this;
        }

        if ("$comment".equals(name)) {
            comment(value.toString());
        } else if ("$explain".equals(name)) {
            findModel.getModifiers().append("$explain", BsonBoolean.TRUE);
        } else if ("$hint".equals(name)) {
            if (value instanceof String) {
                hint((String) value);
            } else {
                hint((DBObject) value);
            }
        } else if ("$maxScan".equals(name)) {
            maxScan(((Number) value).intValue());
        } else if ("$maxTimeMS".equals(name)) {
            maxTime(((Number) value).longValue(), MILLISECONDS);
        } else if ("$max".equals(name)) {
            max((DBObject) value);
        } else if ("$min".equals(name)) {
            min((DBObject) value);
        } else if ("$orderby".equals(name)) {
            sort((DBObject) value);
        } else if ("$returnKey".equals(name)) {
            returnKey();
        } else if ("$showDiskLoc".equals(name)) {
            showDiskLoc();
        } else if ("$snapshot".equals(name)) {
            snapshot();
        } else if ("$natural".equals(name)) {
            sort(new BasicDBObject("$natural", ((Number) value).intValue()));
        } else {
            throw new IllegalArgumentException(name + "is not a supported modifier");
        }
        return this;
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
        findModel.getModifiers().append("$comment", new BsonString(comment));
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
        findModel.getModifiers().append("$maxScan", new BsonInt32(max));
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
        findModel.getModifiers().append("$max", collection.wrap(max));
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
        findModel.getModifiers().append("$min", collection.wrap(min));
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
        findModel.getModifiers().append("$returnKey", BsonBoolean.TRUE);
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
        findModel.getModifiers().append("$showDiskLoc", BsonBoolean.TRUE);
        return this;
    }

    /**
     * Informs the database of indexed fields of the collection in order to improve performance.
     *
     * @param indexKeys a {@code DBObject} with fields and direction
     * @return same DBCursor for chaining operations
     */
    public DBCursor hint(final DBObject indexKeys) {
        findModel.getModifiers().append("$hint", collection.wrap(indexKeys));
        return this;
    }

    /**
     * Informs the database of an indexed field of the collection in order to improve performance.
     *
     * @param indexName the name of an index
     * @return same DBCursor for chaining operations
     */
    public DBCursor hint(final String indexName) {
        findModel.getModifiers().append("$hint", new BsonString(indexName));
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
     * @since 2.12.0
     */
    public DBCursor maxTime(final long maxTime, final TimeUnit timeUnit) {
        findModel.maxTime(maxTime, timeUnit);
        return this;
    }

    /**
     * Use snapshot mode for the query. Snapshot mode assures no duplicates are returned, or objects missed, which were present at both the
     * start and end of the query's execution (if an object is new during the query, or deleted during the query, it may or may not be
     * returned, even with snapshot mode). Note that short query responses (less than 1MB) are always effectively snapshot. Currently,
     * snapshot mode may not be used with sorting or explicit hints.
     *
     * @return {@code this} so calls can be chained
     */
    public DBCursor snapshot() {
        findModel.getModifiers().append("$snapshot", BsonBoolean.TRUE);
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
    public DBObject explain() {
        FindModel<BsonDocument> explainModel = new FindModel<BsonDocument>(findModel);
        explainModel.getModifiers().append("$explain", BsonBoolean.TRUE);
        if (explainModel.getLimit() > 0) {
            // need to pass a negative batchSize as limit for explain
            explainModel.batchSize(explainModel.getLimit() * -1);
            explainModel.limit(0);
        }
        return collection.execute(getQueryOperation(explainModel, collection.getObjectCodec()), getReadPreference()).next();
    }

    private QueryOperation<DBObject> getQueryOperation(final FindModel<BsonDocument> find, final Decoder<DBObject> decoder) {
        return new QueryOperation<DBObject>(collection.getNamespace(), decoder)
                   .criteria(find.getCriteria())
                   .batchSize(find.getBatchSize())
                   .cursorFlags(find.getCursorFlags())
                   .limit(find.getLimit())
                   .maxTime(find.getMaxTime(MILLISECONDS), MILLISECONDS)
                   .modifiers(find.getModifiers())
                   .projection(find.getProjection())
                   .skip(find.getSkip())
                   .sort(find.getSort());
    }

    /**
     * Sorts this cursor's elements. This method must be called before getting any object from the cursor.
     *
     * @param orderBy the fields by which to sort
     * @return a cursor pointing to the first element of the sorted results
     */
    public DBCursor sort(final DBObject orderBy) {
        findModel.sort(collection.wrap(orderBy));
        return this;
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
        findModel.limit(limit);
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
    public DBCursor batchSize(final int numberOfElements) {
        findModel.batchSize(numberOfElements);
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
        findModel.skip(numberOfElements);
        return this;
    }

    @Override
    public long getCursorId() {
        if (cursor != null) {
            return cursor.getServerCursor().getId();
        } else {
            return 0;
        }
    }

    /**
     * Returns the number of objects through which the cursor has iterated.
     *
     * @return the number of objects seen
     */
    public int numSeen() {
        return numSeen;
    }

    @Override
    public void close() {
        closed = true;
        if (cursor != null) {
            cursor.close();
            cursor = null;
        }

        currentObject = null;
    }

    /**
     * Declare that this query can run on a secondary server.
     *
     * @return a copy of the same cursor (for chaining)
     * @see ReadPreference#secondaryPreferred()
     * @deprecated Replaced with {@link com.mongodb.ReadPreference#secondaryPreferred()}
     */
    @Deprecated
    public DBCursor slaveOk() {
        return addOption(Bytes.QUERYOPTION_SLAVEOK);
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
     * Counts the number of objects matching the query. This does not take limit/skip into consideration, and does initiate a call to the
     * server.
     *
     * @return the number of objects
     * @throws MongoException
     * @see DBCursor#size
     */
    public int count() {
        return (int) collection.getCount(getQuery(), getKeysWanted(), 0, 0, getReadPreference(),
                                         findModel.getMaxTime(MILLISECONDS), MILLISECONDS, findModel.getModifiers().get("$hint"));
    }

    /**
     * @return the first matching document
     */
    public DBObject one() {
        return collection.findOne(getQuery(), getKeysWanted(),
                                  findModel.getSort() == null ? null : DBObjects.toDBObject(findModel.getSort()),
                                  getReadPreference(), findModel.getMaxTime(MILLISECONDS), MILLISECONDS);
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
        checkCursorType(CursorType.ARRAY);
        fillArray(Integer.MAX_VALUE);
        return all.size();
    }

    /**
     * For testing only! Iterates cursor and counts objects
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
        return (int) collection.getCount(getQuery(), getKeysWanted(), findModel.getLimit(), findModel.getSkip(), getReadPreference(),
                                         findModel.getMaxTime(MILLISECONDS), MILLISECONDS);
    }

    /**
     * Gets the fields to be returned.
     *
     * @return the field selector that cursor used
     */
    public DBObject getKeysWanted() {
        return findModel.getProjection() == null ? null : DBObjects.toDBObject(findModel.getProjection());
    }

    /**
     * Gets the query.
     *
     * @return the query that cursor used
     */
    public DBObject getQuery() {
        return DBObjects.toDBObject(findModel.getCriteria());
    }

    /**
     * Gets the collection.
     *
     * @return the collection that data is pulled from
     */
    public DBCollection getCollection() {
        return collection;
    }

    @Override
    public ServerAddress getServerAddress() {
        if (cursor != null) {
            return cursor.getServerAddress();
        } else {
            return null;
        }
    }

    /**
     * Sets the read preference for this cursor. See the documentation for {@link ReadPreference} for more information.
     *
     * @param readPreference read preference to use
     * @return {@code this} so calls can be chained
     */
    public DBCursor setReadPreference(final ReadPreference readPreference) {
        this.readPreference = readPreference;
        return this;
    }

    /**
     * Gets the default read preference.
     *
     * @return the readPreference used by this cursor
     */
    public ReadPreference getReadPreference() {
        return readPreference;
    }

    public DBCursor setDecoderFactory(final DBDecoderFactory factory) {
        this.decoderFactory = factory;

        //Not creating new CompoundDBObjectCodec because we don't care about encoder.
        this.resultDecoder = new DBDecoderAdapter(factory.create(), collection, getCollection().getBufferPool());
        return this;
    }

    public DBDecoderFactory getDecoderFactory() {
        return decoderFactory;
    }

    @Override
    public String toString() {
        return "DBCursor{"
               + "collection=" + collection
               + ", find=" + findModel
               + (cursor != null ? (", cursor=" + cursor.getServerCursor()) : "")
               + '}';
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

        if (findModel.getProjection() != null && !findModel.getProjection().isEmpty()) {
            currentObject.markAsPartialObject();
        }

        return currentObject;
    }

    private static DBObject lookupSuitableHints(final DBObject query, final List<DBObject> hints) {
        if (hints == null) {
            return null;
        }

        Set<String> keys = query.keySet();

        for (final DBObject hint : hints) {
            if (keys.containsAll(hint.keySet())) {
                return hint;
            }
        }
        return null;
    }

    private static enum CursorType {
        ITERATOR,
        ARRAY
    }

    private class OptionalFinalizer {
        @Override
        protected void finalize() {
            if (cursor != null) {
                ServerCursor serverCursor = cursor.getServerCursor();
                if (serverCursor != null) {
                    getCollection().getDB().getMongo().addOrphanedCursor(serverCursor);
                }
            }
        }
    }

}
