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

package com.mongodb.client.model;

import com.mongodb.CursorType;
import org.bson.conversions.Bson;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;

/**
 * The options to apply to a find operation (also commonly referred to as a query).
 *
 * @since 3.0
 * @mongodb.driver.manual tutorial/query-documents/ Find
 * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
 */
public final class FindOptions {
    private int batchSize;
    private int limit;
    private Bson modifiers;
    private Bson projection;
    private long maxTimeMS;
    private long maxAwaitTimeMS;
    private int skip;
    private Bson sort;
    private CursorType cursorType = CursorType.NonTailable;
    private boolean noCursorTimeout;
    private boolean oplogReplay;
    private boolean partial;
    private Collation collation;

    /**
     * Construct a new instance.
     */
    public FindOptions() {
    }

    /**
     * Construct a new instance by making a shallow copy of the given model.
     * @param from model to copy
     */
    public FindOptions(final FindOptions from) {
        batchSize = from.batchSize;
        limit = from.limit;
        modifiers = from.modifiers;
        projection = from.projection;
        maxTimeMS = from.maxTimeMS;
        maxAwaitTimeMS = from.maxAwaitTimeMS;
        skip = from.skip;
        sort = from.sort;
        cursorType = from.cursorType;
        noCursorTimeout = from.noCursorTimeout;
        oplogReplay = from.oplogReplay;
        partial = from.partial;
    }

    /**
     * Gets the limit to apply.  The default is null.
     *
     * @return the limit
     * @mongodb.driver.manual reference/method/cursor.limit/#cursor.limit Limit
     */
    public int getLimit() {
        return limit;
    }

    /**
     * Sets the limit to apply.
     *
     * @param limit the limit, which may be null
     * @return this
     * @mongodb.driver.manual reference/method/cursor.limit/#cursor.limit Limit
     */
    public FindOptions limit(final int limit) {
        this.limit = limit;
        return this;
    }

    /**
     * Gets the number of documents to skip.  The default is 0.
     *
     * @return the number of documents to skip, which may be null
     * @mongodb.driver.manual reference/method/cursor.skip/#cursor.skip Skip
     */
    public int getSkip() {
        return skip;
    }

    /**
     * Sets the number of documents to skip.
     *
     * @param skip the number of documents to skip
     * @return this
     * @mongodb.driver.manual reference/method/cursor.skip/#cursor.skip Skip
     */
    public FindOptions skip(final int skip) {
        this.skip = skip;
        return this;
    }

    /**
     * Gets the maximum execution time on the server for this operation.  The default is 0, which places no limit on the execution time.
     *
     * @param timeUnit the time unit to return the result in
     * @return the maximum execution time in the given time unit
     * @mongodb.driver.manual reference/method/cursor.maxTimeMS/#cursor.maxTimeMS Max Time
     */
    public long getMaxTime(final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        return timeUnit.convert(maxTimeMS, TimeUnit.MILLISECONDS);
    }

    /**
     * Sets the maximum execution time on the server for this operation.
     *
     * @param maxTime  the max time
     * @param timeUnit the time unit, which may not be null
     * @return this
     * @mongodb.driver.manual reference/method/cursor.maxTimeMS/#cursor.maxTimeMS Max Time
     */
    public FindOptions maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        isTrueArgument("maxTime > = 0", maxTime >= 0);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    /**
     * The maximum amount of time for the server to wait on new documents to satisfy a tailable cursor
     * query. This only applies to a TAILABLE_AWAIT cursor. When the cursor is not a TAILABLE_AWAIT cursor,
     * this option is ignored.
     *
     * On servers &gt;= 3.2, this option will be specified on the getMore command as "maxTimeMS". The default
     * is no value: no "maxTimeMS" is sent to the server with the getMore command.
     *
     * On servers &lt; 3.2, this option is ignored, and indicates that the driver should respect the server's default value
     *
     * A zero value will be ignored.
     *
     * @param timeUnit the time unit to return the result in
     * @return the maximum await execution time in the given time unit
     * @since 3.2
     * @mongodb.driver.manual reference/method/cursor.maxTimeMS/#cursor.maxTimeMS Max Time
     */
    public long getMaxAwaitTime(final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        return timeUnit.convert(maxAwaitTimeMS, TimeUnit.MILLISECONDS);
    }

    /**
     * Sets the maximum await execution time on the server for this operation.
     *
     * @param maxAwaitTime  the max await time.  A zero value will be ignored, and indicates that the driver should respect the server's
     *                      default value
     * @param timeUnit the time unit, which may not be null
     * @return this
     * @since 3.2
     * @mongodb.driver.manual reference/method/cursor.maxTimeMS/#cursor.maxTimeMS Max Time
     */
    public FindOptions maxAwaitTime(final long maxAwaitTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        isTrueArgument("maxAwaitTime > = 0", maxAwaitTime >= 0);
        this.maxAwaitTimeMS = TimeUnit.MILLISECONDS.convert(maxAwaitTime, timeUnit);
        return this;
    }

    /**
     * Gets the number of documents to return per batch.  Default to 0, which indicates that the server chooses an appropriate batch
     * size.
     *
     * @return the batch size, which may be null
     * @mongodb.driver.manual reference/method/cursor.batchSize/#cursor.batchSize Batch Size
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * Sets the number of documents to return per batch.
     *
     * @param batchSize the batch size
     * @return this
     * @mongodb.driver.manual reference/method/cursor.batchSize/#cursor.batchSize Batch Size
     */
    public FindOptions batchSize(final int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    /**
     * Gets the query modifiers to apply to this operation.  The default is not to apply any modifiers.
     *
     * @return the query modifiers, which may be null
     * @mongodb.driver.manual reference/operator/query-modifier/ Query Modifiers
     */
    public Bson getModifiers() {
        return modifiers;
    }

    /**
     * Sets the query modifiers to apply to this operation.
     *
     * @param modifiers the query modifiers to apply, which may be null.
     * @return this
     * @mongodb.driver.manual reference/operator/query-modifier/ Query Modifiers
     */
    public FindOptions modifiers(final Bson modifiers) {
        this.modifiers = modifiers;
        return this;
    }

    /**
     * Gets a document describing the fields to return for all matching documents.
     *
     * @return the project document, which may be null
     * @mongodb.driver.manual reference/method/db.collection.find/ Projection
     */
    public Bson getProjection() {
        return projection;
    }

    /**
     * Sets a document describing the fields to return for all matching documents.
     *
     * @param projection the project document, which may be null.
     * @return this
     * @mongodb.driver.manual reference/method/db.collection.find/ Projection
     */
    public FindOptions projection(final Bson projection) {
        this.projection = projection;
        return this;
    }

    /**
     * Gets the sort criteria to apply to the query. The default is null, which means that the documents will be returned in an undefined
     * order.
     *
     * @return a document describing the sort criteria
     * @mongodb.driver.manual reference/method/cursor.sort/ Sort
     */
    public Bson getSort() {
        return sort;
    }

    /**
     * Sets the sort criteria to apply to the query.
     *
     * @param sort the sort criteria, which may be null.
     * @return this
     * @mongodb.driver.manual reference/method/cursor.sort/ Sort
     */
    public FindOptions sort(final Bson sort) {
        this.sort = sort;
        return this;
    }

    /**
     * The server normally times out idle cursors after an inactivity period (10 minutes)
     * to prevent excess memory use.  If true, that timeout is disabled.
     *
     * @return true if cursor timeout is disabled
     */
    public boolean isNoCursorTimeout() {
        return noCursorTimeout;
    }

    /**
     * The server normally times out idle cursors after an inactivity period (10 minutes)
     * to prevent excess memory use. Set this option to prevent that.
     *
     * @param noCursorTimeout true if cursor timeout is disabled
     * @return this
     */
    public FindOptions noCursorTimeout(final boolean noCursorTimeout) {
        this.noCursorTimeout = noCursorTimeout;
        return this;
    }

    /**
     * Users should not set this under normal circumstances.
     *
     * @return if oplog replay is enabled
     */
    public boolean isOplogReplay() {
        return oplogReplay;
    }

    /**
     * Users should not set this under normal circumstances.
     *
     * @param oplogReplay if oplog replay is enabled
     * @return this
     */
    public FindOptions oplogReplay(final boolean oplogReplay) {
        this.oplogReplay = oplogReplay;
        return this;
    }

    /**
     * Get partial results from a sharded cluster if one or more shards are unreachable (instead of throwing an error).
     *
     * @return if partial results for sharded clusters is enabled
     */
    public boolean isPartial() {
        return partial;
    }

    /**
     * Get partial results from a sharded cluster if one or more shards are unreachable (instead of throwing an error).
     *
     * @param partial if partial results for sharded clusters is enabled
     * @return this
     */
    public FindOptions partial(final boolean partial) {
        this.partial = partial;
        return this;
    }

    /**
     * Get the cursor type.
     *
     * @return the cursor type
     */
    public CursorType getCursorType() {
        return cursorType;
    }

    /**
     * Sets the cursor type.
     *
     * @param cursorType the cursor type
     * @return this
     */
    public FindOptions cursorType(final CursorType cursorType) {
        this.cursorType = notNull("cursorType", cursorType);
        return this;
    }

    /**
     * Returns the collation options
     *
     * @return the collation options
     * @since 3.4
     * @mongodb.server.release 3.4
     */
    public Collation getCollation() {
        return collation;
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
    public FindOptions collation(final Collation collation) {
        this.collation = collation;
        return this;
    }

    @Override
    public String toString() {
        return "FindOptions{"
               + ", batchSize=" + batchSize
               + ", limit=" + limit
               + ", modifiers=" + modifiers
               + ", projection=" + projection
               + ", maxTimeMS=" + maxTimeMS
               + ", skip=" + skip
               + ", sort=" + sort
               + ", cursorType=" + cursorType
               + ", noCursorTimeout=" + noCursorTimeout
               + ", oplogReplay=" + oplogReplay
               + ", partial=" + partial
               + ", collation=" + collation
               + '}';
    }
}
