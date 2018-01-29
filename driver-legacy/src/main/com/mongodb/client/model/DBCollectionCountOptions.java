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
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;

/**
 * The options for a count operation.
 *
 * @since 3.4
 * @mongodb.driver.manual reference/command/count/ Count
 */
public class DBCollectionCountOptions {
    private DBObject hint;
    private String hintString;
    private int limit;
    private int skip;
    private long maxTimeMS;
    private ReadPreference readPreference;
    private ReadConcern readConcern;
    private Collation collation;

    /**
     * Construct a new instance
     */
    public DBCollectionCountOptions() {
    }

    /**
     * Gets the hint to apply.
     *
     * @return the hint, which should describe an existing
     */
    public DBObject getHint() {
        return hint;
    }

    /**
     * Gets the hint string to apply.
     *
     * @return the hint string, which should be the name of an existing index
     */
    public String getHintString() {
        return hintString;
    }

    /**
     * Sets the hint to apply.
     *
     * @param hint a document describing the index which should be used for this operation.
     * @return this
     */
    public DBCollectionCountOptions hint(final DBObject hint) {
        this.hint = hint;
        return this;
    }

    /**
     * Sets the hint to apply.
     *
     * @param hint the name of the index which should be used for the operation
     * @return this
     */
    public DBCollectionCountOptions hintString(final String hint) {
        this.hintString = hint;
        return this;
    }

    /**
     * Gets the limit to apply.  The default is 0, which means there is no limit.
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
     * @param limit the limit
     * @return this
     * @mongodb.driver.manual reference/method/cursor.limit/#cursor.limit Limit
     */
    public DBCollectionCountOptions limit(final int limit) {
        this.limit = limit;
        return this;
    }

    /**
     * Gets the number of documents to skip.  The default is 0.
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
    public DBCollectionCountOptions skip(final int skip) {
        this.skip = skip;
        return this;
    }

    /**
     * Sets the limit to apply.
     *
     * @param limit the limit
     * @return this
     * @mongodb.driver.manual reference/method/cursor.limit/#cursor.limit Limit
     */
    public DBCollectionCountOptions limit(final long limit) {
        isTrue("limit is too large", limit <= Integer.MAX_VALUE);
        this.limit = (int) limit;
        return this;
    }

    /**
     * Sets the number of documents to skip.
     *
     * @param skip the number of documents to skip
     * @return this
     * @mongodb.driver.manual reference/method/cursor.skip/#cursor.skip Skip
     */
    public DBCollectionCountOptions skip(final long skip) {
        isTrue("skip is too large", skip <= Integer.MAX_VALUE);
        this.skip = (int) skip;
        return this;
    }

    /**
     * Gets the maximum execution time on the server for this operation.  The default is 0, which places no limit on the execution time.
     *
     * @param timeUnit the time unit to return the result in
     * @return the maximum execution time in the given time unit
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
     */
    public DBCollectionCountOptions maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    /**
     * Returns the readPreference
     *
     * @return the readPreference
     */
    public ReadPreference getReadPreference() {
        return readPreference;
    }

    /**
     * Sets the readPreference
     *
     * @param readPreference the readPreference
     * @return this
     */
    public DBCollectionCountOptions readPreference(final ReadPreference readPreference) {
        this.readPreference = readPreference;
        return this;
    }

    /**
     * Returns the readConcern
     *
     * @return the readConcern
     * @mongodb.server.release 3.2
     */
    public ReadConcern getReadConcern() {
        return readConcern;
    }

    /**
     * Sets the readConcern
     *
     * @param readConcern the readConcern
     * @return this
     * @mongodb.server.release 3.2
     */
    public DBCollectionCountOptions readConcern(final ReadConcern readConcern) {
        this.readConcern = readConcern;
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
    public DBCollectionCountOptions collation(final Collation collation) {
        this.collation = collation;
        return this;
    }
}

