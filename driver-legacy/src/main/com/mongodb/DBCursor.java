/*
 * Copyright 2008-present MongoDB, Inc.
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
import com.mongodb.client.MongoCursor;
import com.mongodb.client.internal.MongoBatchCursorAdapter;
import com.mongodb.client.internal.OperationExecutor;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.DBCollectionCountOptions;
import com.mongodb.client.model.DBCollectionFindOptions;
import com.mongodb.internal.connection.PowerOfTwoBufferPool;
import com.mongodb.internal.operation.FindOperation;
import com.mongodb.lang.Nullable;
import org.bson.BsonString;
import org.bson.codecs.Decoder;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import static com.mongodb.MongoClient.getDefaultCodecRegistry;
import static com.mongodb.assertions.Assertions.notNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * <p>An iterator over database results. Doing a {@code find()} query on a collection returns a {@code DBCursor}.</p>
 * <p> An application should ensure that a cursor is closed in all circumstances, e.g. using a try-with-resources statement:</p>
 * <blockquote><pre>
 *    try (DBCursor cursor = collection.find(query)) {
 *        while (cursor.hasNext()) {
 *            System.out.println(cursor.next();
 *        }
 *    }
 * </pre></blockquote>
 *
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
 * See {@link MongoClient#getDB(String)} for further information about the effective deprecation of this class.
 *
 * @mongodb.driver.manual core/read-operations Read Operations
 */
@NotThreadSafe
public class DBCursor implements Cursor, Iterable<DBObject> {
    private final DBCollection collection;
    private final DBObject filter;
    private final DBCollectionFindOptions findOptions;
    private final OperationExecutor executor;
    private final boolean retryReads;
    private DBDecoderFactory decoderFactory;
    private Decoder<DBObject> decoder;
    private IteratorOrArray iteratorOrArray;
    private DBObject currentObject;
    private int numSeen;
    private boolean closed;
    private final List<DBObject> all = new ArrayList<>();
    private MongoCursor<DBObject> cursor;
    private DBCursorCleaner optionalCleaner;

    /**
     * Initializes a new database cursor.
     *
     * @param collection     collection to use
     * @param query          the query filter to apply
     * @param fields         keys to return from the query
     * @param readPreference the read preference for this query
     */
    public DBCursor(final DBCollection collection, final DBObject query, @Nullable final DBObject fields,
                    @Nullable final ReadPreference readPreference) {
        this(collection, query, fields, readPreference, true);
    }

    /**
     * Initializes a new database cursor.
     *
     * @param collection     collection to use
     * @param query          the query filter to apply
     * @param fields         keys to return from the query
     * @param readPreference the read preference for this query
     * @param retryReads     true if reads should be retried
     */
    public DBCursor(final DBCollection collection, final DBObject query, @Nullable final DBObject fields,
                    @Nullable final ReadPreference readPreference, final boolean retryReads) {
        this(collection, query, new DBCollectionFindOptions().projection(fields).readPreference(readPreference), retryReads);
    }

    DBCursor(final DBCollection collection, @Nullable final DBObject filter, final DBCollectionFindOptions findOptions) {
        this(collection, filter, findOptions, true);
    }

    DBCursor(final DBCollection collection, @Nullable final DBObject filter, final DBCollectionFindOptions findOptions,
             final boolean retryReads) {
        this(collection, filter, findOptions, collection.getExecutor(), collection.getDBDecoderFactory(),
                collection.getObjectCodec(), retryReads);
    }

    private DBCursor(final DBCollection collection, @Nullable final DBObject filter, final DBCollectionFindOptions findOptions,
                     final OperationExecutor executor, final DBDecoderFactory decoderFactory, final Decoder<DBObject> decoder,
                     final boolean retryReads) {
        this.collection = notNull("collection", collection);
        this.filter = filter;
        this.executor = notNull("executor", executor);
        this.findOptions = notNull("findOptions", findOptions.copy());
        this.decoderFactory = decoderFactory;
        this.decoder = notNull("decoder", decoder);
        this.retryReads = retryReads;
    }

    /**
     * Creates a copy of an existing database cursor. The new cursor is an iterator, even if the original was an array.
     *
     * @return the new cursor
     */
    public DBCursor copy() {
        return new DBCursor(collection, filter, findOptions, executor, decoderFactory, decoder, retryReads);
    }

    /**
     * Checks if there is another object available.
     *
     * <p><em>Note</em>: Automatically turns cursors of type Tailable to TailableAwait. For non-blocking tailable cursors see
     * {@link #tryNext }.</p>
     *
     * @return true if there is another object available
     * @mongodb.driver.manual /core/cursors/#cursor-batches Cursor Batches
     */
    @Override
    public boolean hasNext() {
        if (closed) {
            throw new IllegalStateException("Cursor has been closed");
        }

        if (cursor == null) {
            FindOperation<DBObject> operation = getQueryOperation(decoder);
            if (operation.getCursorType() == CursorType.Tailable) {
                operation.cursorType(CursorType.TailableAwait);
            }
            initializeCursor(operation);
        }

        boolean hasNext = cursor.hasNext();
        if (cursor.getServerCursor() == null) {
            clearCursorOnCleaner();
        }
        return hasNext;
    }

    /**
     * Returns the object the cursor is at and moves the cursor ahead by one.
     *
     * <p><em>Note</em>: Automatically turns cursors of type Tailable to TailableAwait. For non-blocking tailable cursors see
     * {@link #tryNext }.</p>
     *
     * @return the next element
     * @mongodb.driver.manual /core/cursors/#cursor-batches Cursor Batches
     */
    @Override
    public DBObject next() {
        checkIteratorOrArray(IteratorOrArray.ITERATOR);
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        return nextInternal();
    }

    /**
     * Gets the number of results available locally without blocking, which may be 0.
     *
     * <p>
     * If the cursor is known to be exhausted, returns 0.  If the cursor is closed before it's been exhausted, it may return a non-zero
     * value.
     * </p>
     *
     * @return the number of results available locally without blocking
     * @since 4.5
     */
    public int available() {
        return cursor != null ? cursor.available() : 0;
    }

    /**
     * Non blocking check for tailable cursors to see if another object is available.
     *
     * <p>Returns the object the cursor is at and moves the cursor ahead by one or
     * return null if no documents is available.</p>
     *
     * @return the next element or null
     * @throws MongoException if failed
     * @throws IllegalArgumentException if the cursor is not tailable
     * @mongodb.driver.manual /core/cursors/#cursor-batches Cursor Batches
     */
    @Nullable
    public DBObject tryNext() {
        if (cursor == null) {
            FindOperation<DBObject> operation = getQueryOperation(decoder);
            if (!operation.getCursorType().isTailable()) {
                throw new IllegalArgumentException("Can only be used with a tailable cursor");
            }
            initializeCursor(operation);
        }
        DBObject next = cursor.tryNext();
        if (cursor.getServerCursor() == null) {
            clearCursorOnCleaner();
        }
        return currentObject(next);
    }


    /**
     * Returns the element the cursor is at.
     *
     * @return the current element
     */
    public DBObject curr() {
        return currentObject;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    /**
     * Gets the query limit.
     *
     * @return the limit, or 0 if no limit is set
     */
    public int getLimit() {
        return findOptions.getLimit();
    }

    /**
     * Gets the batch size.
     *
     * @return the batch size
     */
    public int getBatchSize() {
        return findOptions.getBatchSize();
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
        findOptions.comment(comment);
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
        findOptions.max(max);
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
        findOptions.min(min);
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
        findOptions.returnKey(true);
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
        findOptions.hint(indexKeys);
        return this;
    }

    /**
     * Informs the database of an indexed field of the collection in order to improve performance.
     *
     * @param indexName the name of an index
     * @return same DBCursor for chaining operations
     * @since 4.4
     * @mongodb.driver.manual reference/operator/meta/hint/ $hint
     */
    public DBCursor hint(final String indexName) {
        findOptions.hintString(indexName);
        return this;
    }

    /**
     * Set the maximum execution time for operations on this cursor.
     *
     * @param maxTime  the maximum time that the server will allow the query to run, before killing the operation.
     * @param timeUnit the time unit
     * @return same DBCursor for chaining operations
     * @mongodb.driver.manual reference/operator/meta/maxTimeMS/ $maxTimeMS
     * @since 2.12.0
     */
    public DBCursor maxTime(final long maxTime, final TimeUnit timeUnit) {
        findOptions.maxTime(maxTime, timeUnit);
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
     * @throws MongoException if the operation failed
     * @mongodb.driver.manual reference/command/explain Explain Output
     * @mongodb.server.release 3.0
     */
    @Deprecated
    public DBObject explain() {
        return executor.execute(getQueryOperation(collection.getObjectCodec())
                        .asExplainableOperation(null, getDefaultCodecRegistry().get(DBObject.class)),
                getReadPreference(), getReadConcern());
    }

    /**
     * Sets the cursor type.
     *
     * @param cursorType the cursor type, which may not be null
     * @return this
     * @since 3.9
     */
    public DBCursor cursorType(final CursorType cursorType) {
        findOptions.cursorType(cursorType);
        return this;
    }

    /**
     * Users should not set this under normal circumstances.
     *
     * @param oplogReplay if oplog replay is enabled
     * @return this
     * @since 3.9
     * @deprecated oplogReplay has been deprecated in MongoDB 4.4.
     */
    @Deprecated
    public DBCursor oplogReplay(final boolean oplogReplay) {
        findOptions.oplogReplay(oplogReplay);
        return this;
    }

    /**
     * The server normally times out idle cursors after an inactivity period (10 minutes)
     * to prevent excess memory use. Set this option to prevent that.
     *
     * @param noCursorTimeout true if cursor timeout is disabled
     * @return this
     * @since 3.9
     */
    public DBCursor noCursorTimeout(final boolean noCursorTimeout) {
        findOptions.noCursorTimeout(noCursorTimeout);
        return this;
    }

    /**
     * Get partial results from a sharded cluster if one or more shards are unreachable (instead of throwing an error).
     *
     * @param partial if partial results for sharded clusters is enabled
     * @return this
     * @since 3.9
     */
    public DBCursor partial(final boolean partial) {
        findOptions.partial(partial);
        return this;
    }

    @SuppressWarnings("deprecation")
    private FindOperation<DBObject> getQueryOperation(final Decoder<DBObject> decoder) {

        return new FindOperation<>(collection.getNamespace(), decoder)
                                                .filter(collection.wrapAllowNull(filter))
                                                .batchSize(findOptions.getBatchSize())
                                                .skip(findOptions.getSkip())
                                                .limit(findOptions.getLimit())
                                                .maxAwaitTime(findOptions.getMaxAwaitTime(MILLISECONDS), MILLISECONDS)
                                                .maxTime(findOptions.getMaxTime(MILLISECONDS), MILLISECONDS)
                                                .projection(collection.wrapAllowNull(findOptions.getProjection()))
                                                .sort(collection.wrapAllowNull(findOptions.getSort()))
                                                .collation(findOptions.getCollation())
                                                .comment(findOptions.getComment() != null
                                                        ? new BsonString(findOptions.getComment()) : null)
                                                .hint(findOptions.getHint() != null
                                                        ? collection.wrapAllowNull(findOptions.getHint())
                                                        : (findOptions.getHintString() != null
                                                        ? new BsonString(findOptions.getHintString()) : null))
                                                .min(collection.wrapAllowNull(findOptions.getMin()))
                                                .max(collection.wrapAllowNull(findOptions.getMax()))
                                                .cursorType(findOptions.getCursorType())
                                                .noCursorTimeout(findOptions.isNoCursorTimeout())
                                                .oplogReplay(findOptions.isOplogReplay())
                                                .partial(findOptions.isPartial())
                                                .returnKey(findOptions.isReturnKey())
                                                .showRecordId(findOptions.isShowRecordId())
                                                .retryReads(retryReads);
    }

    /**
     * Sorts this cursor's elements. This method must be called before getting any object from the cursor.
     *
     * @param orderBy the fields by which to sort
     * @return a cursor pointing to the first element of the sorted results
     */
    public DBCursor sort(final DBObject orderBy) {
        findOptions.sort(orderBy);
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
        findOptions.limit(limit);
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
        findOptions.batchSize(numberOfElements);
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
        findOptions.skip(numberOfElements);
        return this;
    }

    @Override
    public long getCursorId() {
        if (cursor != null) {
            ServerCursor serverCursor = cursor.getServerCursor();
            if (serverCursor == null) {
                return 0;
            }
            return serverCursor.getId();
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
            clearCursorOnCleaner();
        }

        currentObject = null;
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
     * @throws MongoException if failed
     */
    public List<DBObject> toArray() {
        return toArray(Integer.MAX_VALUE);
    }

    /**
     * Converts this cursor to an array.
     *
     * @param max the maximum number of objects to return
     * @return an array of objects
     * @throws MongoException if failed
     */
    public List<DBObject> toArray(final int max) {
        checkIteratorOrArray(IteratorOrArray.ARRAY);
        fillArray(max - 1);
        return all;
    }

    /**
     * Counts the number of objects matching the query. This does not take limit/skip into consideration, and does initiate a call to the
     * server.
     *
     * @return the number of objects
     * @throws MongoException if the operation failed
     * @see DBCursor#size
     */
    public int count() {
        DBCollectionCountOptions countOptions = getDbCollectionCountOptions();
        return (int) collection.getCount(getQuery(), countOptions);
    }

    /**
     * Returns the first document that matches the query.
     *
     * @return the first matching document
     * @since 2.12
     */
    @Nullable
    public DBObject one() {
        try (DBCursor findOneCursor = copy().limit(-1)) {
            return findOneCursor.hasNext() ? findOneCursor.next() : null;
        }
    }

    /**
     * Pulls back all items into an array and returns the number of objects. Note: this can be resource intensive.
     *
     * @return the number of elements in the array
     * @throws MongoException if failed
     * @see #count()
     * @see #size()
     */
    public int length() {
        checkIteratorOrArray(IteratorOrArray.ARRAY);
        fillArray(Integer.MAX_VALUE);
        return all.size();
    }

    /**
     * For testing only! Iterates cursor and counts objects
     *
     * @return num objects
     * @throws MongoException if failed
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
     * @throws MongoException if the operation failed
     * @see #count()
     */
    public int size() {
        DBCollectionCountOptions countOptions = getDbCollectionCountOptions().skip(findOptions.getSkip()).limit(findOptions.getLimit());
        return (int) collection.getCount(getQuery(), countOptions);
    }

    /**
     * Gets the fields to be returned.
     *
     * @return the field selector that cursor used
     */
    @Nullable
    public DBObject getKeysWanted() {
        return findOptions.getProjection();
    }

    /**
     * Gets the query.
     *
     * @return the query that cursor used
     */
    public DBObject getQuery() {
        return filter;
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
    @Nullable
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
        findOptions.readPreference(readPreference);
        return this;
    }

    /**
     * Gets the default read preference.
     *
     * @return the readPreference used by this cursor
     */
    public ReadPreference getReadPreference() {
        ReadPreference readPreference = findOptions.getReadPreference();
        if (readPreference != null) {
            return readPreference;
        }
        return collection.getReadPreference();
    }


    /**
     * Sets the read concern for this collection.
     *
     * @param readConcern the read concern to use for this collection
     * @since 3.2
     * @mongodb.server.release 3.2
     * @mongodb.driver.manual reference/readConcern/ Read Concern
     */
    DBCursor setReadConcern(@Nullable final ReadConcern readConcern) {
        findOptions.readConcern(readConcern);
        return this;
    }

    /**
     * Get the read concern for this collection.
     *
     * @return the {@link com.mongodb.ReadConcern}
     * @since 3.2
     * @mongodb.server.release 3.2
     * @mongodb.driver.manual reference/readConcern/ Read Concern
     */
    ReadConcern getReadConcern() {
        ReadConcern readConcern = findOptions.getReadConcern();
        if (readConcern != null) {
            return readConcern;
        }
        return collection.getReadConcern();
    }

    /**
     * Returns the collation options
     *
     * @return the collation options
     * @since 3.4
     * @mongodb.server.release 3.4
     */
    @Nullable
    public Collation getCollation() {
        return findOptions.getCollation();
    }

    /**
     * Sets the collation options
     *
     * <p>A null value represents the server default.</p>
     * @param collation the collation options to use
     * @return this
     * @since 3.4
     * @mongodb.server.release 3.4
     */
    public DBCursor setCollation(@Nullable final Collation collation) {
        findOptions.collation(collation);
        return this;
    }

    /**
     * Sets the factory that will be used create a {@code DBDecoder} that will be used to decode BSON documents into DBObject instances.
     *
     * @param factory the DBDecoderFactory
     * @return {@code this} so calls can be chained
     */
    public DBCursor setDecoderFactory(final DBDecoderFactory factory) {
        this.decoderFactory = factory;

        //Not creating new CompoundDBObjectCodec because we don't care about encoder.
        this.decoder = new DBDecoderAdapter(factory.create(), collection, PowerOfTwoBufferPool.DEFAULT);
        return this;
    }

    /**
     * Gets the decoder factory that creates the decoder this cursor will use to decode objects from MongoDB.
     *
     * @return the decoder factory.
     */
    public DBDecoderFactory getDecoderFactory() {
        return decoderFactory;
    }

    @Override
    public String toString() {
        return "DBCursor{"
               + "collection=" + collection
               + ", find=" + findOptions
               + (cursor != null ? (", cursor=" + cursor.getServerCursor()) : "")
               + '}';
    }

    private void initializeCursor(final FindOperation<DBObject> operation) {
        cursor = new MongoBatchCursorAdapter<>(executor.execute(operation, getReadPreference(), getReadConcern()));
        ServerCursor serverCursor = cursor.getServerCursor();
        if (isCursorFinalizerEnabled() && serverCursor != null) {
            optionalCleaner = DBCursorCleaner.create(collection.getDB().getMongoClient(), collection.getNamespace(),
                    serverCursor);
        }
    }

    private void clearCursorOnCleaner() {
        if (optionalCleaner != null) {
            optionalCleaner.clearCursor();
        }
    }

    private boolean isCursorFinalizerEnabled() {
        return collection.getDB().getMongoClient().getMongoClientOptions().isCursorFinalizerEnabled();
    }

    private void checkIteratorOrArray(final IteratorOrArray expected) {
        if (iteratorOrArray == null) {
            iteratorOrArray = expected;
            return;
        }

        if (expected == iteratorOrArray) {
            return;
        }

        throw new IllegalArgumentException("Can't switch cursor access methods");
    }

    private void fillArray(final int n) {
        checkIteratorOrArray(IteratorOrArray.ARRAY);
        while (n >= all.size() && hasNext()) {
            all.add(nextInternal());
        }
    }

    private DBObject nextInternal() {
        if (iteratorOrArray == null) {
            checkIteratorOrArray(IteratorOrArray.ITERATOR);
        }

        DBObject next = cursor.next();
        if (cursor.getServerCursor() == null) {
            clearCursorOnCleaner();
        }
        return currentObjectNonNull(next);
    }

    @Nullable
    private DBObject currentObject(@Nullable final DBObject newCurrentObject){
        if (newCurrentObject != null) {
            currentObject = newCurrentObject;
            numSeen++;

            DBObject projection = findOptions.getProjection();
            if (projection != null && !(projection.keySet().isEmpty())) {
                currentObject.markAsPartialObject();
            }
        }
        return newCurrentObject;
    }

    private DBObject currentObjectNonNull(final DBObject newCurrentObject){
        currentObject = newCurrentObject;
        numSeen++;

        DBObject projection = findOptions.getProjection();
        if (projection != null && !(projection.keySet().isEmpty())) {
            currentObject.markAsPartialObject();
        }
        return newCurrentObject;
    }

    private enum IteratorOrArray {
        ITERATOR,
        ARRAY
    }

    private DBCollectionCountOptions getDbCollectionCountOptions() {
        return new DBCollectionCountOptions()
                .readPreference(getReadPreference())
                .readConcern(getReadConcern())
                .collation(getCollation())
                .maxTime(findOptions.getMaxTime(MILLISECONDS), MILLISECONDS)
                .hint(findOptions.getHint())
                .hintString(findOptions.getHintString());
    }
}
