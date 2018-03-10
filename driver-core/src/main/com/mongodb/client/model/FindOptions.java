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

package com.mongodb.client.model;

import com.mongodb.CursorType;
import com.mongodb.lang.Nullable;
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
    private String comment;
    private Bson hint;
    private Bson max;
    private Bson min;
    private long maxScan;
    private boolean returnKey;
    private boolean showRecordId;
    private boolean snapshot;

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
        comment = from.comment;
        hint = from.hint;
        max = from.max;
        min = from.min;
        maxScan = from.maxScan;
        returnKey = from.returnKey;
        showRecordId = from.showRecordId;
        snapshot = from.snapshot;
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
     * Gets the number of documents to skip. The default is 0.
     *
     * @return the number of documents to skip
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
     * @return the batch size
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
     * @deprecated use the individual modifier methods instead.
     */
    @Deprecated
    @Nullable
    public Bson getModifiers() {
        return modifiers;
    }

    /**
     * Sets the query modifiers to apply to this operation.
     *
     * @param modifiers the query modifiers to apply, which may be null.
     * @return this
     * @mongodb.driver.manual reference/operator/query-modifier/ Query Modifiers
     * @deprecated use the individual modifier methods instead.
     */
    @Deprecated
    public FindOptions modifiers(@Nullable final Bson modifiers) {
        this.modifiers = modifiers;
        return this;
    }

    /**
     * Gets a document describing the fields to return for all matching documents.
     *
     * @return the project document, which may be null
     * @mongodb.driver.manual reference/method/db.collection.find/ Projection
     */
    @Nullable
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
    public FindOptions projection(@Nullable final Bson projection) {
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
    @Nullable
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
    public FindOptions sort(@Nullable final Bson sort) {
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
    @Nullable
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
    public FindOptions collation(@Nullable final Collation collation) {
        this.collation = collation;
        return this;
    }

    /**
     * Returns the comment to send with the query. The default is not to include a comment with the query.
     *
     * @return the comment
     * @since 3.5
     */
    @Nullable
    public String getComment() {
        return comment;
    }

    /**
     * Sets the comment to the query. A null value means no comment is set.
     *
     * @param comment the comment
     * @return this
     * @since 3.5
     */
    public FindOptions comment(@Nullable final String comment) {
        this.comment = comment;
        return this;
    }

    /**
     * Returns the hint for which index to use. The default is not to set a hint.
     *
     * @return the hint
     * @since 3.5
     */
    @Nullable
    public Bson getHint() {
        return hint;
    }

    /**
     * Sets the hint for which index to use. A null value means no hint is set.
     *
     * @param hint the hint
     * @return this
     * @since 3.5
     */
    public FindOptions hint(@Nullable final Bson hint) {
        this.hint = hint;
        return this;
    }

    /**
     * Returns the exclusive upper bound for a specific index. By default there is no max bound.
     *
     * @return the max
     * @since 3.5
     */
    @Nullable
    public Bson getMax() {
        return max;
    }

    /**
     * Sets the exclusive upper bound for a specific index. A null value means no max is set.
     *
     * @param max the max
     * @return this
     * @since 3.5
     */
    public FindOptions max(@Nullable final Bson max) {
        this.max = max;
        return this;
    }

    /**
     * Returns the minimum inclusive lower bound for a specific index. By default there is no min bound.
     *
     * @return the min
     * @since 3.5
     */
    @Nullable
    public Bson getMin() {
        return min;
    }

    /**
     * Sets the minimum inclusive lower bound for a specific index. A null value means no max is set.
     *
     * @param min the min
     * @return this
     * @since 3.5
     */
    public FindOptions min(@Nullable final Bson min) {
        this.min = min;
        return this;
    }

    /**
     * Returns the maximum number of documents or index keys to scan when executing the query.
     *
     * A zero value or less will be ignored, and indicates that the driver should respect the server's default value.
     *
     * @return the maxScan
     * @since 3.5
     */
    public long getMaxScan() {
        return maxScan;
    }

    /**
     * Sets the maximum number of documents or index keys to scan when executing the query.
     *
     * A zero value or less will be ignored, and indicates that the driver should respect the server's default value.
     *
     * @param maxScan the maxScan
     * @return this
     * @since 3.5
     */
    public FindOptions maxScan(final long maxScan) {
        this.maxScan = maxScan;
        return this;
    }

    /**
     * Returns the returnKey. If true the find operation will return only the index keys in the resulting documents.
     *
     * Default value is false. If returnKey is true and the find command does not use an index, the returned documents will be empty.
     *
     * @return the returnKey
     * @since 3.5
     */
    public boolean isReturnKey() {
        return returnKey;
    }

    /**
     * Sets the returnKey. If true the find operation will return only the index keys in the resulting documents.
     *
     * @param returnKey the returnKey
     * @return this
     * @since 3.5
     */
    public FindOptions returnKey(final boolean returnKey) {
        this.returnKey = returnKey;
        return this;
    }

    /**
     * Returns the showRecordId.
     *
     * Determines whether to return the record identifier for each document. If true, adds a field $recordId to the returned documents.
     * The default is false.
     *
     * @return the showRecordId
     * @since 3.5
     */
    public boolean isShowRecordId() {
        return showRecordId;
    }

    /**
     * Sets the showRecordId. Set to true to add a field {@code $recordId} to the returned documents.
     *
     * @param showRecordId the showRecordId
     * @return this
     * @since 3.5
     */
    public FindOptions showRecordId(final boolean showRecordId) {
        this.showRecordId = showRecordId;
        return this;
    }

    /**
     * Returns the snapshot.
     *
     * Prevents the cursor from returning a document more than once because of an intervening write operation. The default is false.
     *
     * @return the snapshot
     * @since 3.5
     */
    public boolean isSnapshot() {
        return snapshot;
    }

    /**
     * Sets the snapshot.
     *
     * If true it prevents the cursor from returning a document more than once because of an intervening write operation.
     *
     * @param snapshot the snapshot
     * @return this
     * @since 3.5
     */
    public FindOptions snapshot(final boolean snapshot) {
        this.snapshot = snapshot;
        return this;
    }

    @Override
    public String toString() {
        return "FindOptions{"
                + "batchSize=" + batchSize
                + ", limit=" + limit
                + ", modifiers=" + modifiers
                + ", projection=" + projection
                + ", maxTimeMS=" + maxTimeMS
                + ", maxAwaitTimeMS=" + maxAwaitTimeMS
                + ", skip=" + skip
                + ", sort=" + sort
                + ", cursorType=" + cursorType
                + ", noCursorTimeout=" + noCursorTimeout
                + ", oplogReplay=" + oplogReplay
                + ", partial=" + partial
                + ", collation=" + collation
                + ", comment='" + comment + "'"
                + ", hint=" + hint
                + ", max=" + max
                + ", min=" + min
                + ", maxScan=" + maxScan
                + ", returnKey=" + returnKey
                + ", showRecordId=" + showRecordId
                + ", snapshot=" + snapshot
                + "}";
    }
}
