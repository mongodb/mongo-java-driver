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

import com.mongodb.DBObject;
import com.mongodb.WriteConcern;
import com.mongodb.lang.Nullable;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;

/**
 * The oprtions for find and modify operations.
 *
 * @since 3.4
 */
public final class DBCollectionFindAndModifyOptions {
    private DBObject projection;
    private DBObject sort;
    private boolean remove;
    private DBObject update;
    private boolean upsert;
    private boolean returnNew;
    private Boolean bypassDocumentValidation;
    private long maxTimeMS;
    private WriteConcern writeConcern;
    private Collation collation;
    private List<? extends DBObject> arrayFilters;

    /**
     * Construct a new instance
     */
    public DBCollectionFindAndModifyOptions() {
    }

    /**
     * Returns the projection
     *
     * @return the projection
     */
    @Nullable
    public DBObject getProjection() {
        return projection;
    }

    /**
     * Sets the projection
     *
     * @param projection the projection
     * @return this
     */
    public DBCollectionFindAndModifyOptions projection(@Nullable final DBObject projection) {
        this.projection = projection;
        return this;
    }

    /**
     * Returns the sort
     *
     * @return the sort
     */
    @Nullable
    public DBObject getSort() {
        return sort;
    }

    /**
     * Sets the sort
     *
     * @param sort the sort
     * @return this
     */
    public DBCollectionFindAndModifyOptions sort(@Nullable final DBObject sort) {
        this.sort = sort;
        return this;
    }

    /**
     * Returns the remove
     *
     * @return the remove
     */
    public boolean isRemove() {
        return remove;
    }

    /**
     * Sets the remove
     *
     * @param remove the remove
     * @return this
     */
    public DBCollectionFindAndModifyOptions remove(final boolean remove) {
        this.remove = remove;
        return this;
    }

    /**
     * Returns the update
     *
     * @return the update
     */
    @Nullable
    public DBObject getUpdate() {
        return update;
    }

    /**
     * Sets the update
     *
     * @param update the update
     * @return this
     */
    public DBCollectionFindAndModifyOptions update(@Nullable final DBObject update) {
        this.update = update;
        return this;
    }

    /**
     * Returns the upsert
     *
     * @return the upsert
     */
    public boolean isUpsert() {
        return upsert;
    }

    /**
     * Sets the upsert
     *
     * @param upsert the upsert
     * @return this
     */
    public DBCollectionFindAndModifyOptions upsert(final boolean upsert) {
        this.upsert = upsert;
        return this;
    }

    /**
     * Returns the returnNew
     *
     * @return the returnNew
     */
    public boolean returnNew() {
        return returnNew;
    }

    /**
     * Sets the returnNew
     *
     * @param returnNew the returnNew
     * @return this
     */
    public DBCollectionFindAndModifyOptions returnNew(final boolean returnNew) {
        this.returnNew = returnNew;
        return this;
    }

    /**
     * Returns the bypassDocumentValidation
     *
     * @return the bypassDocumentValidation
     */
    public Boolean getBypassDocumentValidation() {
        return bypassDocumentValidation;
    }

    /**
     * Sets the bypassDocumentValidation
     *
     * @param bypassDocumentValidation the bypassDocumentValidation
     * @return this
     */
    public DBCollectionFindAndModifyOptions bypassDocumentValidation(final Boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
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
    public DBCollectionFindAndModifyOptions maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        isTrueArgument("maxTime > = 0", maxTime >= 0);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    /**
     * Returns the writeConcern
     *
     * @return the writeConcern
     * @mongodb.server.release 3.2
     */
    @Nullable
    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    /**
     * Sets the writeConcern
     *
     * @param writeConcern the writeConcern
     * @return this
     * @mongodb.server.release 3.2
     */
    public DBCollectionFindAndModifyOptions writeConcern(@Nullable final WriteConcern writeConcern) {
        this.writeConcern = writeConcern;
        return this;
    }

    /**
     * Returns the collation options
     *
     * @return the collation options
     * @mongodb.server.release 3.4
     */
    public Collation getCollation() {
        return collation;
    }

    /**
     * Sets the collation
     *
     * @param collation the collation
     * @return this
     * @mongodb.server.release 3.4
     */
    public DBCollectionFindAndModifyOptions collation(final Collation collation) {
        this.collation = collation;
        return this;
    }

    /**
     * Sets the array filters option
     *
     * @param arrayFilters the array filters, which may be null
     * @return this
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    public DBCollectionFindAndModifyOptions arrayFilters(final List<? extends DBObject> arrayFilters) {
        this.arrayFilters = arrayFilters;
        return this;
    }

    /**
     * Returns the array filters option
     *
     * @return the array filters, which may be null
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    @Nullable
    public List<? extends DBObject> getArrayFilters() {
        return arrayFilters;
    }
}
